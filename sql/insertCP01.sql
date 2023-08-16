insert into db.FINDB(eventDate, MR, cpName, shoulder, TariffPlanName, RatingResult, MSISDN, IMSI, origToNumber, MSCID, recType, service, timeKey, dateTime, source, duration, volume, gmtoffset, ratingRes, isRoam ,dch
inputFileName, origTime, udf, cellid, lac, region, ratingPlan, reference, filterGroup ,record_id,LOADED, date, time)
with    'CH01' as v_CHName                                               -- номер 
        dictGet('db.dic_CH', 'days_before', v_CHName) as v_days_before, -- количество дней вычитаемых из текущего, для определения периода выгрузки
        ifNull((select if (count() >= 1000000, 10000, 9999999999)
                    from db.STARTDB d where
                    toDate(eventDate)= today() v_days_before
                    and d.cpName v_CHName
                    and d.shoulder = 1
                    and d.iflag = 0
                ), 9999999999) as lim,                                  -- если кол-во записей больше 1кк, то обрезаем по 10к записей в час для абонента
        ifNull((select if(count() >= 1000000, 1000000, 9999999999)
                    from db.STARTDB d where
                    toDate(eventDate) = today() v_days_before
                    and d.cpName v_CHName and d.shoulder = 1
                    and d.iflag = 0
                ), 9999999999) as maxlim                                -- максимум 1кк записей выгружаем из 2 STARTDB при тех же условиях
select distinct t1.eventDate, t1.MR, t1.cpName, t1.shoulder, t1.TariffPlanName, t1.RatingResult, t1.MSISDN, t1.IMSI, t1.origToNumber, t1.MSCID, t1.recType, t1.service, t1.timeKey 
,t1.dateTime,t1. source, t1. duration, t1.volume, t1.gmtOffset, t1.ratingRes, t1.isRoam ,t1.dch
,if(lim-10000, concat('!', t1.inputFileName), t1.inputFileName) as inputFileName, t1.origTime
,t1.udf, t1.cellid, t1.lac, t1.region, t1.ratingPlan, t1.reference, t1.filterGroup ,t1.record_id,t1.LOADED, t1.date, t1.time
from (select substring(dateTime,1,10) as `date`
        ,substring(dateTime,12,8) as `time`
        ,eventDate, MR, cpName, shoulder, TariffPlanName, RatingResult, MSISDN, IMSI, origToNumber, MSCID, recType, service, timeKey ,dateTime, source, duration, volume, gmtoffset
        ,ratingRes, isRoam,dch, inputFileName, origTime, udf, cellid, lac, region ,ratingPlan, reference, filterGroup, record_id, LOADED
        from db.STARTDB
        where 1=1
        and eventDate = today ()-v_days_before
        and cpName = v_CHName
        and iflag = 0
        limit lim by toHour (eventDateTime)
        limit maxlim
        ) t1
left join (select distinct IMSI, origToNumber, service, recType, duration, dateTime, t2.dateTime as dateTime2
            from (select IMSI, origToNumber, service, recType, duration, dateTime, eventDateTime
                    from db.STARTDB
                    where 1=1
                    and eventDate today ()-v_days_before
                    and cpName = v_CHName
                    and iflag = 0
                    limit lim by toHour (eventDateTime)
                    limit maxlim
                ) t1
        left join (SELECT distinct IMSI, origToNumber, service, recType, duration, dateTime
                    from db.RNDDB gbl
                    where 1=1
                    and eventDate = today ()-v_days_before
                    and IMSI in (select IMSI
                                from db.STARTDB ds 
                                where 1=1
                                and eventDate = today ()-v_days_before
                                and cpName = v_CHName
                                and iflag = 0)
                    ) t2 on t1.IMSI=t2.IMSI and t1.origToNumber=t2.origToNumber and t1.service-t2.service and t1.recType=t2.recType 
        where 1=1
        and abs(t1.duration-t2.duration) <3
        and abs (parseDateTimeBestEffortOrZero(t1.dateTime)-parseDateTimeBestEffortOrZero(t2.dateTime)) <3
        ) t2 on t1.IMSI = t2.IMSI and t1.origToNumber=t2.origToNumber and t1.service-t2.service and t1.recType=t2.recType and t1.dateTime=t2.dateTime

where 1=1
and not (concat(t1. IMSI, t1. origToNumber, t1. service, t1.recType) = concat(t2.IMSI, t2.origToNumber, t2.service, t2.recType)
        and abs (parseDateTimeBestEffortOrZero (t1.dateTime) - parseDateTimeBestEffortOrZero (t2.dateTime2)) <3)