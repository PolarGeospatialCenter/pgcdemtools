create view dem.scene_dem_noncanonical as
SELECT a.scenedemid,
       a.stripdemid,
       a.pairname,
       a.sensor1,
       a.sensor2,
       a.acqdate1,
       a.acqdate2,
       a.catalogid1,
       a.catalogid2,
       a.dem_res,
       a.algm_ver,
       a.prod_ver,
       a.scene1,
       a.scene2,
       a.gen_time1,
       a.gen_time2,
       a.is_dsp,
       a.is_xtrack
FROM dem.scene_dem_all a
         LEFT JOIN dem.scene_dem_master b ON a.stripdemid = b.stripdemid AND a.is_dsp = b.is_dsp
WHERE b.stripdemid IS NULL
  AND a.is_dsp = false;

comment on  view dem.scene_dem_noncanonical is 'DEMs in scene_dem_all (non-DSP) that are not in scene_dem_master. DEM locations can be obtained by joining to scene_dem and scene_dem_staging tables.';

alter  view dem.scene_dem_noncanonical owner to pgc_gis_admin;

grant select on dem.scene_dem_noncanonical to pgc_users;

grant select on dem.scene_dem_noncanonical to backup;

