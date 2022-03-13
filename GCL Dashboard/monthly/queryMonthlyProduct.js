var MonthIndex = [0,1,2,3,4,5,6,7,8,9,10,11];
var MonthNameIndex = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

datetime.setHours(23);
datetime.setMinutes(59);
datetime.setSeconds(59);
datetime.setMonth(11);
datetime.setDate(31);
var dateTimeStr = dateFormat(datetime, "YYYY-MM-dd HH:mm:ss");

datetime.setHours(0);
datetime.setMinutes(0);
datetime.setSeconds(0);
datetime.setMonth(0);
datetime.setDate(1);
var dateTimeEnd = dateFormat(datetime, "YYYY-MM-dd HH:mm:ss");

var query = "select \
production_plant, \
sum(cast(mlt.value as float)) as sum, \
extract(month from mlt.created_at) as mm \
from mes_master_data.operation_lots ol \
inner join mes_master_data.operation_lot_activities ola on \
ol.operation_lot_id = ola.operation_lot_id \
inner join mes_master_data.machine_lots_transaction mlt on \
ola.operation_lot_activitiy_id = mlt.operation_lot_activity_id \
where ol.lot_status = 'Done' and ol.machine_type = 'palletizer' and ol.production_plant in ('LLDPE1','LLDPE2','LDPE') \
and ol.actual_start >= '"+dateTimeEnd+"' and ol.actual_finish <= '"+dateTimeStr+"' and \
mlt.machine_lots_parameter_id = 72 \
group by ol.production_plant, mm";

//Query Total Bagging Weight Palleizer 
var SmallBagQueryTable =  Things["GCL_WP_MES_Database"].SQLQuery({
	query: query /* STRING */
});


var BigBagQuery = "select \
production_plant, \
ol.material_code, \
sum(cast(mlt.value as float)) as sum, \
extract(month from mlt.created_at) as mm \
from mes_master_data.operation_lots ol \
inner join mes_master_data.operation_lot_activities ola on \
ol.operation_lot_id = ola.operation_lot_id \
inner join mes_master_data.machine_lots_transaction mlt on  \
ola.operation_lot_activitiy_id = mlt.operation_lot_activity_id  \
where ol.lot_status = 'Done' and ol.machine_type = 'big bag' and ol.production_plant in ('LLDPE1','LLDPE2','LDPE')  \
and ol.actual_start >= '"+dateTimeEnd+"' and ol.actual_finish <= '"+dateTimeStr+"' and  \
mlt.machine_lots_parameter_id = 104 \
group by ol.production_plant, ol.material_code, mm";

//Query Total Bagging Number (BigBag)
var BigBagQueryTable =  Things["GCL_WP_MES_Database"].SQLQuery({
	query: BigBagQuery /* STRING */
});

if(BigBagQueryTable.length > 0)
{
	for(i=0;i<BigBagQueryTable.length;i++)
    {
       	
        // result: INFOTABLE dataShape: ""
        var SearchBigBagPackingVolume =  Things["GCL_WP_SS_02_MaterialCode_Configuration"].SearchDataTableEntries({
        	maxItems: undefined /* NUMBER */,
        	searchExpression: BigBagQueryTable[i].material_code.substring(0, 2) /* STRING */,
        	query: undefined /* QUERY */,
        	source: undefined /* STRING */,
        	tags: undefined /* TAGS */
        }); 
        
        if(SearchBigBagPackingVolume.length > 0)
        {
        	BigBagQueryTable[i].sum = BigBagQueryTable[i].sum * SearchBigBagPackingVolume.packing_volume;
        }
        
		else
        {
        	var SearchBigBagPackingVolume2 =  Things["GCL_WP_SS_01_SCADA_Log_Sheet_MatCode_Matching"].SearchDataTableEntries({
        	maxItems: undefined /* NUMBER */,
        	searchExpression: BigBagQueryTable[i].material_code.substring(0, 2) /* STRING */,
        	query: undefined /* QUERY */,
        	source: undefined /* STRING */,
        	tags: undefined /* TAGS */
        	});
            
            if(SearchBigBagPackingVolume2.length > 0)
            {
            	BigBagQueryTable[i].sum = (BigBagQueryTable[i].sum * SearchBigBagPackingVolume2.packing_volumns * 1000).toFixed(2);
            }
        }
    }
}

//Sum BigBag Weight
var AggregateParams = {
t: BigBagQueryTable /* INFOTABLE */,
columns: 'sum' /* STRING */,
aggregates: "sum" /* STRING */,
groupByColumns: 'mm,production_plant' /* STRING */
};

// result: INFOTABLE
BigBagQueryTable = Resources["InfoTableFunctions"].Aggregate(AggregateParams);

////Rename sum_sum field
var RenameParams = {
	t: BigBagQueryTable /* INFOTABLE */,
	from: 'sum_sum' /* STRING */,
	to: 'sum' /* STRING */
};

// result: INFOTABLE
BigBagQueryTable = Resources["InfoTableFunctions"].RenameField(RenameParams);


var UnionParams = {
	t1: SmallBagQueryTable /* INFOTABLE */,
	t2: BigBagQueryTable /* INFOTABLE */
};

// result: INFOTABLE
var UnionTable = Resources["InfoTableFunctions"].Union(UnionParams);

//Sum BigBag And SmallBag
var AllAggregateParams = {
t: UnionTable /* INFOTABLE */,
columns: 'sum' /* STRING */,
aggregates: "sum" /* STRING */,
groupByColumns: 'mm,production_plant' /* STRING */
};

// result: INFOTABLE
UnionTable = Resources["InfoTableFunctions"].Aggregate(AllAggregateParams);

//Combine Table
var params = {
    infoTableName : "InfoTable",
    dataShapeName : "GCL_MESD_Product_ProductChart_DataShape"
};
// CreateInfoTableFromDataShape(infoTableName:STRING("InfoTable"), dataShapeName:STRING):INFOTABLE(GCL_WP_MES_031_FiilingQuantity_DataShape)
var FinishInfoTable = Resources["InfoTableFunctions"].CreateInfoTableFromDataShape(params);

//Function Filter Weight By Plant & Month
function filterWeightByPlant (Plant_Name,Month_Value) {
    var FilterQuery = {
        "filters":{
            "type":"AND",
            "filters":[
                {
                    "fieldName": "production_plant",
                    "type":"EQ",
                    "value": Plant_Name
                },
                {
                    "fieldName": "mm",
                    "type":"EQ",
                    "value":  Month_Value
                }
                ]
        }
    };
    
    var QueryParams = {
	t: UnionTable /* INFOTABLE */,
	query: FilterQuery /* QUERY */
	};
    
    // result: INFOTABLE
	var FilterInfotable = Resources["InfoTableFunctions"].Query(QueryParams);
    return FilterInfotable;

}

//Combine BigBag and SmallBag
for(i=0;i<12;i++)
{
	// GCL_WP_MES_031_FiilingQuantity_DataShape entry object
	var newEntry = new Object();
	newEntry.date = MonthNameIndex[i];
    newEntry.Total = 0;
    newEntry.Target = 0;
    
    //Find Value on Plant LDPE
	newEntry.LDPE = 0; // NUMBER
    var LDPEFilter = filterWeightByPlant("LDPE",i+1);
    if(LDPEFilter.length > 0)
    {
    	newEntry.LDPE = LDPEFilter.sum_sum;
        newEntry.Total += LDPEFilter.sum_sum;
    }
    
   	//Find Value on Plant LLDPE1
	newEntry.LLDPE1 = 0; // NUMBER
	var LLDPE1Filter = filterWeightByPlant("LLDPE1",i+1);
    if(LLDPE1Filter.length > 0)
    {
    	newEntry.LLDPE1 = LLDPE1Filter.sum_sum;
        newEntry.Total += LLDPE1Filter.sum_sum;
    }
    
    //Find Value on Plant LLDPE2   
	newEntry.LLDPE2 = 0; // NUMBER
	var LLDPE2Filter = filterWeightByPlant("LLDPE2",i+1);
    if(LLDPE2Filter.length > 0)
    {
    	newEntry.LLDPE2 = LLDPE2Filter.sum_sum;
        newEntry.Total += LLDPE2Filter.sum_sum;
    }
    
    //Find Production Plan
	newEntry.target = 0; // NUMBER

    var SearchProductionPlan =  Things["Report_ProductionPlanVolume_Master"].SearchDataTableEntries({
    	maxItems: undefined /* NUMBER */,
    	searchExpression: MonthNameIndex[i] + " AND " + datetime.getFullYear() /* STRING */,
    	query: undefined /* QUERY */,
    	source: undefined /* STRING */,
    	tags: undefined /* TAGS */
    }); 
    
    if(SearchProductionPlan.length > 0)
    {
        if(SearchProductionPlan.total_budget != null){
            newEntry.Target = SearchProductionPlan.total_budget;  
        }	   
    } 
    
    FinishInfoTable.AddRow(newEntry);

}

//Changer every Weight KG to KGTon and adjust to 2 decimal
for(i=0;i<FinishInfoTable.length;i++)
{
	FinishInfoTable[i].LDPE = (FinishInfoTable[i].LDPE / 1000000).toFixed(2);
    FinishInfoTable[i].LLDPE1 = (FinishInfoTable[i].LLDPE1 / 1000000).toFixed(2);
    FinishInfoTable[i].LLDPE2 = FinishInfoTable[i].LLDPE2 / 1000000;
    FinishInfoTable[i].Target = FinishInfoTable[i].Target / 1000000;
    FinishInfoTable[i].Total = FinishInfoTable[i].Total / 1000000;
}

result = FinishInfoTable;
