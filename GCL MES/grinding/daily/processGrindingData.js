//---------------------------------------------------------------
// DESCRIPTION
// [Checing this machine have save all operation record information in db]
//---------------------------------------------------------------
// Revision Date Who What
// [INPROGRESS] [2020-12-16] [BVH] Initial Development
//---------------------------------------------------------------
//*********** DEFINE YOUR SERVICE INPUTS/OUTPUT HERE ***********/
/** @type {Datetime} */
var selectedDate;

/** @type {Infotable} */
var result;

(function () {
	// services variable
    
    var datetimeYesterday = new Date(
    	selectedDate.getFullYear(),
        selectedDate.getMonth(),
        selectedDate.getDate(),
        0
    );
    
    var datetimeToday = new Date(
    	selectedDate.getFullYear(),
        selectedDate.getMonth(),
        selectedDate.getDate(),
        23,
        59,
        59
    );
    
    var resultInfotable = Resources["InfoTableFunctions"].CreateInfoTableFromDataShape({
        infoTableName : "InfoTable",
        dataShapeName : "GCL_WP_MES_032_GrindingPulverizerReport_Daily_OEEDictionary_Datashape"
    });
    
    function initResultInfotable(){
        for(let i = 1; i<=6; i++){
            var thingName = "WP-MES-0234_Pulverizer"+i+"_RemoteThing";
            var newEntry = new Object();
            newEntry.id = 17+i;
            newEntry.machine_name = "Pulverizer No "+i;
            newEntry.machine_duration = (Things[thingName].vibratory_feeder_actual).toFixed(2);
            newEntry.AVERAGE_oee = 0;
            newEntry.AVERAGE_availability = 0;
            newEntry.AVERAGE_performance = 0;
            newEntry.AVERAGE_quality = 0;
            newEntry.kwhperton = 0;
            newEntry.oeepers = 0;
            newEntry.availabilitypers = 0;
            newEntry.performancepers = 0;
            newEntry.qualitypers = 0;
            newEntry.SUM_actual_production = 0;
            newEntry.machine_running_duration = 0;
            newEntry.kWton = "";
            newEntry.plant_name = "";
            newEntry.line_name = "";
            newEntry.running_start_date = selectedDate;
            newEntry.running_end_date = datetimeToday;
            newEntry.availability = 0;
            newEntry.performance = 0;
            newEntry.quality = 0;
            newEntry.oee = 0;
            resultInfotable.AddRow(newEntry);
        }
    }
    
    function GetEnergyAllMC(){
        var queryTodayEnergyStr = "\
        	SELECT \
                pmv.power_meter_name, \
                pmv.last_energy_value_daily, \
                pmv.\"timestamp\" \
        	FROM \
            	mes_machine_data.power_meter_value pmv \
        	WHERE \
        		pmv.power_meter_name in ('Q2', 'Q3', 'Q4', 'Q5', 'MC5', 'MC6') \
        		and date_part('year'::text, pmv.\"timestamp\") = "+selectedDate.getFullYear()+" \
                and date_part('month'::text, pmv.\"timestamp\") = "+(selectedDate.getMonth()+1)+" \
                and date_part('day'::text, pmv.\"timestamp\") = "+selectedDate.getDate()+" \
        	ORDER BY pmv.power_meter_name";
        return me.SQLQuery({
                query: queryTodayEnergyStr /* STRING */
        });
    }
    
    function getLotDeatils(){
        var inputStartDate = datetimeYesterday.getFullYear()+'-'+(datetimeYesterday.getMonth()+1)+'-'+datetimeYesterday.getDate()+" 00:00:00";
        var inputFinishDate = datetimeToday.getFullYear()+'-'+(datetimeToday.getMonth()+1)+'-'+datetimeToday.getDate()+" 23:59:59";
    	var queryLotDetailsStr = "\
        select \
            mct.machine_condition_parameter_id, \
            mct.value, \
            ola2.machine_id \
        from mes_master_data.machine_condition_transaction mct \
        left join mes_master_data.operation_lot_activities ola2 on ola2.operation_lot_activitiy_id = mct.operation_lot_activity_id \
        where \
            mct.machine_condition_parameter_id in (1, 8) \
            and mct.created_at between '"+inputStartDate+"' and '"+inputFinishDate+"' \
            and mct.operation_lot_activity_id in ( \
                select \
                    oxx.operation_lot_activitiy_id \
                    from ( \
                        select \
                            distinct on (ola.machine_id) ola.machine_id, \
                            ola.operation_lot_activitiy_id \
                        from \
                            mes_master_data.operated_machines om \
                        left join mes_master_data.operation_lot_activities ola on ola.operation_lot_activitiy_id = om.operation_lot_activitiy_id \
                        where \
                            ola.machine_id in (18, 19, 20, 21, 22, 23) \
                            and om.created_at between '"+inputStartDate+"' and '"+inputFinishDate+"' \
                    )as oxx \
            );";

        return me.SQLQuery({
            query: queryLotDetailsStr /* STRING */
        });
    }

    function getStandDardSpeed(machine_id, product_grade){
        var pulverizer_target_query = "select \
            pgp.c4 as c4, \
            pgp.c6 as c6, \
            pgp.hd as hd \
            from scheduling_master_data.product_grades_pulverizer pgp \
            where pgp.pulverizer_machine_id = "+machine_id+"";

        var pulverizer_target_infotable =  Things["GCL_WP_MES_Database"].SQLQuery({
            query: pulverizer_target_query /* STRING */
        });

        //Check Product Grade
        if(product_grade.substr(0,2) == "HD")
        {
            ProductGrade = "hd";	
        }
        else if(product_grade.substr(3,1) == 4)
        {
            ProductGrade = "c4";
        }
        else if(product_grade.substr(3,1) == 6)
        {
            ProductGrade = "c6";
        }

        return pulverizer_target_infotable[ProductGrade];
    }

    function getQData(operation_lot_id){
        var queryStr = "\
		select \
        	quality \
        from mes_machine_data.oee_transaction ot \
        where ot.operation_lot_id ="+operation_lot_id;
        var qualityValue =  Things["GCL_WP_MES_Database"].SQLQuery({
            query: queryStr /* STRING */
        });
        if(qualityValue.length > 0){
            return qualityValue.quality;
        }else{
            return 1;
        }
    }
	function main(){
        initResultInfotable();
        //Query Performance data
        var performanceData = me.QueryPerformanceAllMC({
            startFilterDateTime: datetimeYesterday /* DATETIME */,
            endFilterDateTime: datetimeToday /* DATETIME */,
            year: selectedDate.getFullYear(),
            month: (selectedDate.getMonth()+1),
            day:selectedDate.getDate(),
        });
        // result = performanceData;
        for(let i = 0; i<performanceData.getRowCount(); i++){
            for(let j = 0; j<resultInfotable.getRowCount(); j++){
                if(performanceData[i].machine_id == resultInfotable[j].id){
                    var qValue = getQData(performanceData[i].operation_lot_id);
                    if(performanceData[i].product_grade !== "" && performanceData[i].product_grade !== undefined){

                    	var stdSpeed = getStandDardSpeed(performanceData[i].machine_id, performanceData[i].product_grade);
             
                        var performance = 
                            (
                                performanceData[i].actual_production / 
                                (performanceData[i].running_time/(3600*1000))
                            ) / stdSpeed;
                        resultInfotable[j].performance += performance 
                            * performanceData[i].actual_production;
                        
                        var availability = performanceData[i].running_time /
                            (
                                performanceData[i].running_time 
                                + performanceData[i].downtime_time
                            )
                        resultInfotable[j].availability += availability 
                            * performanceData[i].actual_production;

                        resultInfotable[j].quality += qValue 
                            * performanceData[i].actual_production;
                        resultInfotable[j].line_name = "STD "+stdSpeed+" Kg/Hr";
                        resultInfotable[j].plant_name = "STD "+(stdSpeed*24/1000).toFixed(2)+" Ton";
                    }
                    //     resultInfotable[j].AVERAGE_performance = (performance*100).toFixed(2);
                    //     var oee = performance*performanceData[i].availability*qValue;
                    //     resultInfotable[j].AVERAGE_oee = (oee*100).toFixed(2);
                    //     resultInfotable[j].oeepers = (oee*100).toFixed(2)+'%';
                    //     resultInfotable[j].performancepers = (performance*100).toFixed(2)+'%';
                    //     resultInfotable[j].line_name = "STD "+stdSpeed+" Kg/Hr";
                    //     resultInfotable[j].plant_name = "STD "+(stdSpeed*24/1000).toFixed(2)+" Ton";
                    // }else{
                    //     resultInfotable[j].AVERAGE_performance = (performanceData[i].performance*100).toFixed(2);
                    //     resultInfotable[j].AVERAGE_oee = (performanceData[i].oee*100).toFixed(2);
                    //     resultInfotable[j].oeepers = (performanceData[i].oee*100).toFixed(2)+'%';
                    //     resultInfotable[j].performancepers = (performanceData[i].performance*100).toFixed(2)+'%';
                    // }

                    // resultInfotable[j].AVERAGE_availability = (performanceData[i].availability*100).toFixed(2);
                    // resultInfotable[j].AVERAGE_quality = (qValue*100).toFixed(2);
                    resultInfotable[j].machine_running_duration += performanceData[i].running_time;
                    resultInfotable[j].SUM_actual_production += (performanceData[i].actual_production/1000);
                    resultInfotable[j].fgPerHr = (resultInfotable[j].SUM_actual_production*1000/(resultInfotable[j].machine_running_duration/1000/60/60)).toFixed(2).toString();

                    // resultInfotable[j].availabilitypers = (performanceData[i].availability*100).toFixed(2)+'%';
                    // resultInfotable[j].qualitypers = (performanceData[i].quality*100).toFixed(2)+'%';
                    break;
                }
            }
        }

        for(let j = 0; j<resultInfotable.getRowCount(); j++){
            var avg_a = resultInfotable[j].availability /
                (resultInfotable[j].SUM_actual_production*1000);
            
            var avg_p = resultInfotable[j].performance /
                (resultInfotable[j].SUM_actual_production*1000);

            var avg_q = resultInfotable[j].quality /
                (resultInfotable[j].SUM_actual_production*1000);
            
            var avg_oee = avg_a * avg_p * avg_q;

            resultInfotable[j].AVERAGE_oee = (avg_oee*100).toFixed(2);
            resultInfotable[j].oeepers = resultInfotable[j].AVERAGE_oee+'%';

            resultInfotable[j].AVERAGE_availability = (avg_a*100).toFixed(2);
            resultInfotable[j].availabilitypers = resultInfotable[j].AVERAGE_availability+'%';

            resultInfotable[j].AVERAGE_performance = (avg_p*100).toFixed(2);
            resultInfotable[j].performancepers = resultInfotable[j].AVERAGE_performance+'%';

            resultInfotable[j].AVERAGE_quality = (avg_q*100).toFixed(2);
            resultInfotable[j].qualitypers = resultInfotable[j].AVERAGE_quality+'%';
            resultInfotable[j].SUM_actual_production = resultInfotable[j].SUM_actual_production.toFixed(2);
        }
        
        var lastedLotInformationData = me.QueryLastedLotInformation({
            startFilterDate: datetimeYesterday /* DATETIME */,
            endFilterDate: datetimeToday /* DATETIME */
        });
        for(let i = 0; i<lastedLotInformationData.getRowCount(); i++){
            for(let j = 0; j<resultInfotable.getRowCount(); j++){
                if(lastedLotInformationData[i].machine_id == resultInfotable[j].id){
                    resultInfotable[j].lotNumber = lastedLotInformationData[i].lot_number_name;
                    resultInfotable[j].productGrade = lastedLotInformationData[i].product_grade;
                    break;
                }
            }
        }
        
        var todayEnergy = GetEnergyAllMC();
        for(let i = 0; i<todayEnergy.getRowCount(); i++){
            if(todayEnergy[i].power_meter_name == "MC5"){
               infoTableIndex = 4;
            }else if(todayEnergy[i].power_meter_name == "MC6"){
               infoTableIndex = 5;
            }else if(todayEnergy[i].power_meter_name == "Q2"){
               infoTableIndex = 0;
            }else if(todayEnergy[i].power_meter_name == "Q3"){
               infoTableIndex = 1;
            }else if(todayEnergy[i].power_meter_name == "Q4"){
               infoTableIndex = 2;
            }else if(todayEnergy[i].power_meter_name == "Q5"){
               infoTableIndex = 3;
            }
            resultInfotable[infoTableIndex].value = (todayEnergy[i].last_energy_value_daily/1000).toFixed(2);
            if(resultInfotable[infoTableIndex].SUM_actual_production > 0)
                resultInfotable[infoTableIndex].kwhperton = (resultInfotable[infoTableIndex].value / resultInfotable[infoTableIndex].SUM_actual_production).toFixed(2);
        }
        
        var dailyLotDetails = getLotDeatils();
        for(let i = 0; i<dailyLotDetails.getRowCount(); i++){
            for(let j = 0; j<resultInfotable.getRowCount(); j++){
                if(dailyLotDetails[i].machine_id == resultInfotable[j].id){
                    if (dailyLotDetails[i].machine_condition_parameter_id == 1){
                        resultInfotable[j].chillerTemp = parseFloat(dailyLotDetails[i].value).toFixed(2);
                    }else if (dailyLotDetails[i].machine_condition_parameter_id == 8){
                        resultInfotable[j].diffPressureGauge = parseFloat(dailyLotDetails[i].value).toFixed(2);
                    }
                    break;
                }
            }
        }
        
        result = resultInfotable;
    }
    
    // call main function
    main();
})();
