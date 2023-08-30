create view strip_dem_gentime(stripdemid, is_dsp, min_gen_time, max_gen_time) as
SELECT scene_dem_all.stripdemid,
       scene_dem_all.is_dsp,
       min(LEAST(scene_dem_all.gen_time1, scene_dem_all.gen_time2))    AS min_gen_time,
       max(GREATEST(scene_dem_all.gen_time1, scene_dem_all.gen_time2)) AS max_gen_time
FROM dem.scene_dem_all
GROUP BY scene_dem_all.stripdemid, scene_dem_all.is_dsp;

alter table strip_dem_gentime
    owner to pgc_gis_admin;

grant select on strip_dem_gentime to pgc_users;