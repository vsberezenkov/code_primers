---author="Berezenkov V.S." 

insert into rlc.3Pass(eventDate,MR, cpName, shoulder, TariffPlanName, RatingResult,MSISDN, IMSI, origToNumber,MSCID, recType, service, timeKey, dateTime, source, duration, volume, gmtOFfset, ratingRes)
with 'Cp06' as v_cpName,
        3 as v_days_before,
        3 as v_days_before_sth
select distinct t1.eventDate,t1.MR,t1.cpName,t1. shoulder, t1.TariffPlanName, t1.RatingResult, t1.MSISDN,t1.IMSI,t1.origToNumber,t1.MSCID, t1.recType, t1.service, t1.timekey ,t1.dateTime, t1.source,t1.duration, t1.volume, t1.gmtOFfset, t1.ratingRes
from (select substring(dateTime,1,10) as date
            ,substring(dateTime,12,8) as time
            ,*
        from rlc.DeltaStream
        where 1=1
        and eventDate = today()-if(MR = 'Sth', v_days_before_sth, v_days_before)
        and cpName = v_cpName
        and iflag = 0
    ) t1
left join (select distinct concat(t1.IMSI,t1.origToNumber,t1.service,t1.recType) as keyPass
                            ,IMSI, origToNumber, service,recType, duration, dateTime, timekey
            from (select IMSI,origToNumber, service, recType, duration, dateTime, timeKey
                    from rlc.DeltaStream
                    where 1=1
                    and eventDate = today()-if(MR = 'Sth', v_days before_sth, v_days_before)
                    and cpName = v_cpName
                    and iflag = 0
                ) t1
            join (SELECT INST, origToNumber, service, recType, duration, dateTime, timeKey
                    from rlc.GpB11_local gbl
                    where 1=1
                    and eventDate between today()-(if(macroregion = 'STH', v_days_before_sth, v_days_before)+1) and today()-(if(macroregion = 'STH', v_days_before_sth, v_days_before)-1)
                    and IMSI in (select IMSI
                                from rlc.DeltaStream ds
                                where
                                and eventDate = today()-if(MR = 'Sth', v_days_before_sth, v_days_before)
                                and cplame = v_cpName
                                and iflag = 0)
                ) t2 on t1.IMSI=t2.IMSI and t1.origToNumber=t2.origToNumber and t1.service=t2.service and t1.recType=t2.recType
            where 1=1
            and abs(t1.duration-t2.duration)<3
            and (abs(t1.timekey-t2.timekey) <3
                or abs((t1.timeKey+3600)-t2.timeKey)<3
                or abs(t1.timekey-(t2.timekey+3600))<3)
            ) t2 on concat(t1.IMSI,t1.origToNumber,t1.service,t1.recType) = t2.keyPass
where 1-1
and concat (t1.IMSI, t1.origTotlumber,t1.service,t1.recType) !=t2.keyPass