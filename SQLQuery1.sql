--exec [stp_RabbitEndOfTripsResults_Get_byTripsID_t] 1150438, 1000084031, 2, 100000, 200000, 9622210
ALTER PROCEDURE [dbo].[stp_RabbitEndOfTripsResults_Get_byTripsID_t]  
(  
    @user_id int,  
 @customer_id int,  
 @language_id int,  
 @fromTripId int,  
 @toTripId int ,
 @carNum nvarchar(50) = null 
)  
AS  
begin  
  
 declare @start_date datetime  
    , @end_date datetime  
  
 select @start_date = StartDate from [dbo].[tbl_RabbitEndOfTripsResults]  
  where [trip_id] = @fromTripId  
  
  
 select @end_date = EndDate from [dbo].[tbl_RabbitEndOfTripsResults]  
  where [trip_id] = @toTripId  
   
DECLARE @sUnknownDriverName  nvarchar(50)      
SELECT @sUnknownDriverName = [translated]      
FROM [dbo].[tbl_Translations]      
where language_id = @language_id and translation_type_id = 139 and translated_id = 346      
  
  
 declare @UserVehicles table  (VehicleId int not null);  
 declare @UserDrivers table  (DriverId int not null, driverName nvarchar(100) null);  
  
 insert into @UserVehicles (VehicleId)  
  select distinct vehicle_id  
  from dbo.fnc_Users_GetVehicles (@user_id, @customer_id, @start_date, @end_date, 84/*fms*/) lu  
  where Use_in_dailyReport = 1 AND VehLabel = ISNULL(@carNum, VehLabel);  
  
  
 insert into @UserDrivers (DriverId, driverName)  
  select DriverId,   
  (case when DriverID = -1   
   then @sUnknownDriverName   
   else  DriverFullName end)  
       as DriverFullName  
  from dbo.fnc_Users_BYDATE_GetDrivers (@user_id, @customer_id, @start_date, @end_date);  
  
   
   
 SELECT [driveNumerator]  
      ,[driverCode]  
      ,[driver]  
      ,[carNum]  
      ,[isNightDriver]  
      ,[totalDriveKm]  
      ,[totalDriveTime]  
      ,[totalStandTime]  
      ,[fastestDriveSpeed]  
   ,CAST(StartDate AS date) AS startDriveDate  
      ,dbo.fnc_onlyTime(StartDate) AS startDriveTime  
      ,[startDriverAddress]  
      ,[startDriveKm]  
      , CAST(endDate AS date) AS endDriveDate  
      ,dbo.fnc_onlyTime(t.endDate) AS endDriveTime  
      ,[endDriverAddress]  
      ,[endDriveKm]  
      ,[carName]  
      ,[returnCode]  
      ,[returnMessage]  
   ,LatStart  
   ,lonStart  
   ,latEnd  
   ,LonEnd  
   from  
 (  
 SELECT [driveNumerator]  
      ,[driverCode]  
      ,ud.driverName as [driver]  
      ,[carNum]  
      ,[isNightDriver]  
      ,[totalDriveKm]  
      ,[totalDriveTime]  
      ,[totalStandTime]  
      ,[fastestDriveSpeed]  
   ,[dbo].[fnc_DBTime2UserTime] (@user_id,StartDate) StartDate  
   --,CAST([dbo].[fnc_DBTime2UserTime] (@user_id,t.StartDate) AS date) AS startDriveDate  
   --   ,dbo.fnc_onlyTime([dbo].[fnc_DBTime2UserTime] (@user_id,t.StartDate)) AS startDriveTime  
      ,[startDriverAddress]  
      ,[startDriveKm]  
      --, CAST([dbo].[fnc_DBTime2UserTime] (@user_id,t.endDate) AS date) AS endDriveDate  
      --,dbo.fnc_onlyTime([dbo].[fnc_DBTime2UserTime] (@user_id,t.endDate)) AS endDriveTime  
   ,[dbo].[fnc_DBTime2UserTime] (@user_id,endDate)endDate  
      ,[endDriverAddress]  
      ,[endDriveKm]  
      ,[carName]  
      ,[returnCode]  
      ,[returnMessage]  
   ,LatStart  
   ,lonStart  
   ,latEnd  
   ,LonEnd  
 from vew_RabbitDrivesTemplate t  
 inner join @UserVehicles uv   
  on  t.[CustomerID] = @customer_id  
  and driveNumerator > @fromTripId and (@toTripId is null or driveNumerator <= @toTripId)  
  and t.PlatformID = uv.VehicleId  
 inner join @UserDrivers ud   
  on t.driverCode = ud.DriverId  
 )t   
  order by [driveNumerator]
   
  
  
END  