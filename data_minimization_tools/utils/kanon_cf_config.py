from cn.protect.hierarchy import OrderHierarchy

cn_config = {
    "start_latitude": ("quasi", OrderHierarchy("interval", 1, 2, 4)),
    "start_longitude": ("quasi", OrderHierarchy("interval", 1, 2, 4)),
    "external_id": ("identifying", None)
}
