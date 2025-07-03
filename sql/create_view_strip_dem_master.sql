create materialized view dem.strip_dem_master as
WITH latest_version AS (
        SELECT left(b.stripdemid, '-8') AS strip_nover,
               max(b.stripdemid)        AS strip_max
        FROM dem.strip_dem_all b
        GROUP BY ("left"(b.stripdemid, '-8'))
     ),
     latest_s2s AS (
         SELECT a.stripdemid,
                array_to_string(max(string_to_array(s2s_ver, '.')::int[]),'.') as max_s2s
         FROM dem.strip_dem_all a
         JOIN latest_version ON a.stripdemid = latest_version.strip_max
         GROUP BY a.stripdemid
     ),
     latest_lsf AS (
         SELECT c.stripdemid,
                c.s2s_ver,
                bool_and(c.is_lsf) AS min_lsf
         FROM dem.strip_dem_all c
         JOIN latest_s2s ON c.stripdemid = latest_s2s.stripdemid and c.s2s_ver = latest_s2s.max_s2s
         GROUP BY c.stripdemid, s2s_ver
     )

SELECT sda.dem_id,
       sda.stripdemid,
       sda.pairname,
       sda.sensor1,
       sda.sensor2,
       sda.acqdate1,
       sda.acqdate2,
       sda.avgacqtm1,
       sda.avgacqtm2,
       sda.catalogid1,
       sda.catalogid2,
       sda.cent_lat,
       sda.cent_lon,
       sda.geocell,
       sda.region,
       sda.epsg,
       sda.proj4,
       sda.nd_value,
       sda.dem_res,
       sda.cr_date,
       sda.algm_ver,
       sda.s2s_ver,
       sda.is_lsf,
       sda.is_xtrack,
       sda.edgemask,
       sda.watermask,
       sda.cloudmask,
       sda.mask_dens,
       sda.valid_dens,
       sda.valid_area,
       sda.valid_perc,
       sda.water_area,
       sda.water_perc,
       sda.cloud_area,
       sda.cloud_perc,
       sda.avgconvang,
       sda.avg_ht_acc,
       sda.avg_sunel1,
       sda.avg_sunel2,
       sda.rmse,
       sda.location,
       sda.filesz_dem,
       sda.filesz_mt,
       sda.filesz_or,
       sda.filesz_or2,
       sda.index_date,
       sda.wkb_geometry
FROM dem.strip_dem_all sda
JOIN latest_lsf
    ON sda.stripdemid = latest_lsf.stripdemid
    AND sda.s2s_ver = latest_lsf.s2s_ver
    AND sda.is_lsf = latest_lsf.min_lsf;

comment on materialized view dem.strip_dem_master is 'Strip DEMs from strip_dem_all that belong to canonical stripdemids and exist on Vida or on tape. Canonical is defined as the latest SETSM version of a stereo imagery pair and resolution. The latest s2s version and the the Non-LSF version is given preference if multiples exist.';

create index strip_dem_mst_dem_id_idx
    on dem.strip_dem_master (dem_id);

create unique index strip_dem_mst_dem_strip_id_idx
    on dem.strip_dem_master (dem_id, stripdemid);

alter materialized view dem.strip_dem_master owner to pgc_gis_admin;

grant select on dem.strip_dem_master to pgc_users;
