from cn.protect.hierarchy import OrderHierarchy

cn_config = {
    "start_latitude": ("quasi", OrderHierarchy("interval", 1, 2, 4)),
    "start_longitude": ("quasi", OrderHierarchy("interval", 1, 2, 4)),
    "external_id": ("identifying", None)
}
longlathierarchy = ("quasi", OrderHierarchy("interval", 1, 2, 4))
cn_config = {
    "start_lat": longlathierarchy,
    "start_long": longlathierarchy,
    "username": ("identifying", None),
    "end_long": longlathierarchy,
    "end_lat": longlathierarchy
}
