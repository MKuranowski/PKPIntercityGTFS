import impuls
import json

class LoadPlatformData(impuls.Task):
    def __init__(self) -> None:
        super().__init__()
    
    @staticmethod
    def load_platforms(path: impuls.tools.types.StrPath) -> None:
        with open(path, "r", encoding="utf-8") as f:
            platforms = json.load(f)
        return platforms

    def execute(self, r: impuls.TaskRuntime) -> None:
        platforms_in_db = r.db.raw_execute(
            """
        SELECT DISTINCT name, stop_id, json_extract(stop_times.extra_fields_json, '$.track') AS track
        FROM stop_times join stops USING (stop_id)
        WHERE track IS NOT NULL and track is not ''
        """
        ).all()
        print(f"Found {len(platforms_in_db)} platforms in DB")
        print(platforms_in_db[:10])
        platforms = self.load_platforms(r.resources["platforms.json"].stored_at)
        for name, stop_id, track in platforms_in_db:
            platform_id = f"{stop_id}_{track}"
            platform = [platform for platform in  platforms.get(name, []) if platform.get("track") == track]
            if len(platform)>0:
                platform = platform[0]
                location = platform.get("location")
                if not location:
                    print(f"Platform {name} track {track} has no location")
                    parent_stop = r.db.retrieve_must(impuls.model.Stop, stop_id)
                    location = [parent_stop.lon, parent_stop.lat]

                r.db.create(
                    impuls.model.Stop(
                        id=platform_id,
                        name=name,
                        parent_station=stop_id,
                        location_type=0, #TODO: set 1 for parent station?
                        platform_code=f"{platform.get('platform')}/{track}",
                        lon=location[0],
                        lat=location[1],
                    )
                )
                r.db.raw_execute(
                    """
                UPDATE stop_times
                SET stop_id = ?
                WHERE stop_id = ? AND json_extract(extra_fields_json, '$.track') = ?
                """,
                    (platform_id, stop_id, track),
                )
            else:
                print(f"Platform not found for {name} track {track}")
