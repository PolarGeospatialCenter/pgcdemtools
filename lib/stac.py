import dataclasses
import pathlib
import rasterio

COLLECTIONS = {
    "arcticdem-mosaics-v3.0-2m",
    "arcticdem-mosaics-v3.0-10m",
    "arcticdem-mosaics-v3.0-32m",
    "arcticdem-mosaics-v4.1-2m",
    "arcticdem-mosaics-v4.1-10m",
    "arcticdem-mosaics-v4.1-32m",
    "arcticdem-strips-s2s041-2m",
    "earthdem-strips-s2s041-2m",
    "rema-mosaics-v2.0-2m",
    "rema-mosaics-v2.0-10m",
    "rema-mosaics-v2.0-32m",
    "rema-strips-s2s041-2m",
}


@dataclasses.dataclass(frozen=True)
class RasterAssetInfo:
    nodata: int | float
    data_type: str
    gsd: float
    proj_code: str
    proj_shape: list[int]
    proj_transform: list[float]
    proj_bbox: list[float]
    proj_geojson: dict
    proj_centroid: list[float]

    @classmethod
    def from_raster(cls, filepath: str | pathlib.Path):
        with rasterio.open(filepath, "r") as src:
            if not src.crs.is_projected:
                raise ValueError(f"{filepath} does not use a projected CRS")

            authority, code = src.crs.to_authority()
            x_min, y_min, x_max, y_max = src.bounds
            proj_geojson = {
                "type": "Polygon",
                "coordinates": [
                    [
                        (x_min, y_min),  # lower left
                        (x_max, y_min),  # lower right
                        (x_max, y_max),  # upper right
                        (x_min, y_max),  # upper left
                        (x_min, y_min),  # lower left again
                    ]
                ],
            }

            centroid_long, centroid_lat = src.lnglat()

            return cls(
                nodata=src.nodata,
                data_type=src.dtypes[0],
                gsd=(src.res[0] + src.res[1]) / 2.0,
                proj_code=f"{authority}:{code}",
                proj_shape=[src.height, src.width],
                proj_transform=list(src.transform),
                proj_bbox=list(src.bounds),
                proj_geojson=proj_geojson,
                # The center of the raster in [lat, long] per the projection extension
                # https://github.com/stac-extensions/projection?tab=readme-ov-file#projcentroid
                proj_centroid=[centroid_lat, centroid_long],
            )
