"""mesh-rest-bridge multi-device server module.

New architecture — leaves core/ untouched:
  module/device_manager.py  — DeviceManager: N simultaneous BLE bridges
  module/server.py          — FastAPI app, device-namespaced routes, CORS, no static files
  module/main.py            — entry point (python -m module.main)
"""
