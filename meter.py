import asyncio
import logging
from asyncio.exceptions import TimeoutError  # Deprecated in 3.11
from dbus_next.aio import MessageBus
from __main__ import VERSION
from __main__ import __file__ as MAIN_FILE
from aiovelib.service import Service, IntegerItem, DoubleItem, TextItem
from aiovelib.service import TextArrayItem
from aiovelib.client import Monitor, ServiceHandler
from aiovelib.localsettings import SettingsService, Setting, SETTINGS_SERVICE

logger = logging.getLogger(__name__)

# Text formatters
unit_watt = lambda v: "{:.0f}W".format(v)
unit_volt = lambda v: "{:.1f}V".format(v)
unit_amp = lambda v: "{:.1f}A".format(v)
unit_kwh = lambda v: "{:.2f}kWh".format(v)
unit_productid = lambda v: "0x{:X}".format(v)

class LocalSettings(SettingsService, ServiceHandler):
    pass

class Meter(object):
    def __init__(self, bus_type):
        self.bus_type = bus_type
        self.monitor = None
        self.service = None
        self.servicePM = [None, None, None]  # Array to hold services for instances 2, 3, and 4
        self.destroyed = False
        self.PMSettingsSetup = False

    async def wait_for_settings(self):
        try:
            return await asyncio.wait_for(self.monitor.wait_for_service(SETTINGS_SERVICE), 5)
        except TimeoutError:
            pass
        return None

    def get_settings(self):
        return self.monitor.get_service(SETTINGS_SERVICE)

    async def start(self, host, port, data):
        try:
            mac = data['result']['mac']
            fw = data['result']['fw_id']
        except KeyError:
            return False

        bus = await MessageBus(bus_type=self.bus_type).connect()
        self.monitor = await Monitor.create(bus, self.settings_changed)

        settingprefix = '/Settings/Devices/shelly_' + mac + '1'
        logger.info("Waiting for localsettings")
        settings = await self.wait_for_settings()
        if settings is None:
            logger.error("Failed to connect to localsettings")
            return False

        logger.info("Connected to localsettings")

        await settings.add_settings(
            Setting(settingprefix + "/ClassAndVrmInstance", "grid:40", 0, 0, alias="instance"),
            Setting(settingprefix + '/Position', 0, 0, 2, alias="position")
        )

        role, instance = self.role_instance(settings.get_value(settings.alias("instance")))

        self.service = await Service.create(bus, "com.victronenergy.{}.shelly_{}".format(role, mac) + '1')

        self.service.add_item(TextItem('/Mgmt/ProcessName', MAIN_FILE))
        self.service.add_item(TextItem('/Mgmt/ProcessVersion', VERSION))
        self.service.add_item(TextItem('/Mgmt/Connection', f"WebSocket {host}:{port}"))
        self.service.add_item(IntegerItem('/DeviceInstance', instance))
        self.service.add_item(IntegerItem('/ProductId', 0xB034, text=unit_productid))
        self.service.add_item(TextItem('/ProductName', "Shelly energy meter + 1"))
        self.service.add_item(TextItem('/FirmwareVersion', fw))
        self.service.add_item(IntegerItem('/Connected', 1))
        self.service.add_item(IntegerItem('/RefreshTime', 100))

        self.service.add_item(TextArrayItem('/AllowedRoles', ['grid', 'pvinverter', 'genset', 'acload']))
        self.service.add_item(TextItem('/Role', role, writeable=True, onchange=self.role_changed))
        self.service.add_item(TextArrayItem('/AllowedDevices', ['em', 'pm']))
        self.service.add_item(TextItem('/DeviceType', role, writeable=True, onchange=self.device_changed))

        if role == 'pvinverter':
            self.service.add_item(IntegerItem('/Position',
                                               settings.get_value(settings.alias("position")),
                                               writeable=True, onchange=self.position_changed))

        self.service.add_item(DoubleItem('/Ac/Energy/Forward', None, text=unit_kwh))
        self.service.add_item(DoubleItem('/Ac/Energy/Reverse', None, text=unit_kwh))
        self.service.add_item(DoubleItem('/Ac/Power', None, text=unit_watt))
        for prefix in (f"/Ac/L{x}" for x in range(1, 4)):
            self.service.add_item(DoubleItem(prefix + '/Voltage', None, text=unit_volt))
            self.service.add_item(DoubleItem(prefix + '/Current', None, text=unit_amp))
            self.service.add_item(DoubleItem(prefix + '/Power', None, text=unit_watt))
            self.service.add_item(DoubleItem(prefix + '/Energy/Forward', None, text=unit_kwh))
            self.service.add_item(DoubleItem(prefix + '/Energy/Reverse', None, text=unit_kwh))

        return True

    def destroy(self):
        if self.PMSettingsSetup:
            if self.service is not None:
                self.service.__del__()
                for service in self.servicePM:
                    if service is not None:
                        service.__del__()
            self.service = None
            self.servicePM = [None, None, None]
            self.settings = None
            self.destroyed = True
        else:
            if self.service is not None:
                self.service.__del__()
            self.service = None
            self.settings = None
            self.destroyed = True

    async def pmSetup(self):
        if self.PMSettingsSetup:
            bus = await MessageBus(bus_type=self.bus_type).connect()

            for instance_number in range(2, 5):
                settingprefix = f'/Settings/Devices/shelly_{mac}{instance_number}'
                await settings.add_settings(
                    Setting(settingprefix + "/ClassAndVrmInstance", "grid:40", 0, 0, alias="instance"),
                    Setting(settingprefix + '/Position', 0, 0, 2, alias="position")
                )

                role, instance = self.role_instance(settings.get_value(settings.alias("instance")))

                self.servicePM[instance_number - 2] = await Service.create(
                    bus, f"com.victronenergy.{role}.shelly_{mac}{instance_number}")

                self.servicePM[instance_number - 2].add_item(TextItem('/Mgmt/ProcessName', MAIN_FILE))
                self.servicePM[instance_number - 2].add_item(TextItem('/Mgmt/ProcessVersion', VERSION))
                self.servicePM[instance_number - 2].add_item(TextItem('/Mgmt/Connection', f"WebSocket {host}:{port}"))
                self.servicePM[instance_number - 2].add_item(IntegerItem('/DeviceInstance', instance + instance_number))
                self.servicePM[instance_number - 2].add_item(IntegerItem('/ProductId', 0xB034, text=unit_productid))
                self.servicePM[instance_number - 2].add_item(TextItem('/ProductName', f"Shelly energy meter + {instance_number}"))
                self.servicePM[instance_number - 2].add_item(TextItem('/FirmwareVersion', fw))
                self.servicePM[instance_number - 2].add_item(IntegerItem('/Connected', 1))
                self.servicePM[instance_number - 2].add_item(IntegerItem('/RefreshTime', 100))

                self.servicePM[instance_number - 2].add_item(TextArrayItem('/AllowedRoles', ['grid', 'pvinverter', 'genset', 'acload']))
                self.servicePM[instance_number - 2].add_item(TextItem('/Role', role, writeable=True, onchange=self.role_changed))
                self.servicePM[instance_number - 2].add_item(TextArrayItem('/AllowedDevices', ['em', 'pm']))
                self.servicePM[instance_number - 2].add_item(TextItem('/DeviceType', role, writeable=True, onchange=self.device_changed))

                if role == 'pvinverter':
                    self.servicePM[instance_number - 2].add_item(IntegerItem('/Position',
                                                                             settings.get_value(settings.alias("position")),
                                                                             writeable=True, onchange=self.position_changed))

                self.servicePM[instance_number - 2].add_item(DoubleItem('/Ac/Energy/Forward', None, text=unit_kwh))
                self.servicePM[instance_number - 2].add_item(DoubleItem('/Ac/Energy/Reverse', None, text=unit_kwh))
                self.servicePM[instance_number - 2].add_item(DoubleItem('/Ac/Power', None, text=unit_watt))
                for prefix in (f"/Ac/L{x}" for x in range(1, 4)):
                    self.servicePM[instance_number - 2].add_item(DoubleItem(prefix + '/Voltage', None, text=unit_volt))
                    self.servicePM[instance_number - 2].add_item(DoubleItem(prefix + '/Current', None, text=unit_amp))
                    self.servicePM[instance_number - 2].add_item(DoubleItem(prefix + '/Power', None, text=unit_watt))
                    self.servicePM[instance_number - 2].add_item(DoubleItem(prefix + '/Energy/Forward', None, text=unit_kwh))
                    self.servicePM[instance_number - 2].add_item(DoubleItem(prefix + '/Energy/Reverse', None, text=unit_kwh))

            self.PMSettingsSetup = True

    async def update(self, data):
        if self.service and data.get('method') == 'NotifyStatus':
            if self.settings.get_value(settings.alias("DeviceType")) == 'em':
                try:
                    d = data['params']['em:0']
                except KeyError:
                    pass
                else:
                    with self.service as s:
                        s['/Ac/L1/Voltage'] = d["a_voltage"]
                        s['/Ac/L2/Voltage'] = d["b_voltage"]
                        s['/Ac/L3/Voltage'] = d["c_voltage"]
                        s['/Ac/L1/Current'] = d["a_current"]
                        s['/Ac/L2/Current'] = d["b_current"]
                        s['/Ac/L3/Current'] = d["c_current"]
                        s['/Ac/L1/Power'] = d["a_act_power"]
                        s['/Ac/L2/Power'] = d["b_act_power"]
                        s['/Ac/L3/Power'] = d["c_act_power"]
                        s['/Ac/Power'] = d["a_act_power"] + d["b_act_power"] + d["c_act_power"]

                try:
                    d = data['params']['emdata:0']
                except KeyError:
                    pass
                else:
                    with self.service as s:
                        s["/Ac/Energy/Forward"] = round(d["total_act"]/1000, 1)
                        s["/Ac/Energy/Reverse"] = round(d["total_act_ret"]/1000, 1)
                        s["/Ac/L1/Energy/Forward"] = round(d["a_total_act_energy"]/1000, 1)
                        s["/Ac/L1/Energy/Reverse"] = round(d["a_total_act_ret_energy"]/1000, 1)
                        s["/Ac/L2/Energy/Forward"] = round(d["b_total_act_energy"]/1000, 1)
                        s["/Ac/L2/Energy/Reverse"] = round(d["b_total_act_ret_energy"]/1000, 1)
                        s["/Ac/L3/Energy/Forward"] = round(d["c_total_act_energy"]/1000, 1)
                        s["/Ac/L3/Energy/Reverse"] = round(d["c_total_act_ret_energy"]/1000, 1)

            elif self.settings.get_value(settings.alias("DeviceType")) == 'pm':
                if self.PMSettingsSetup:
                    for i, service in enumerate(self.servicePM):
                        try:
                            d = data['params']['switch:' + str(i+1)]
                        except KeyError:
                            pass
                        else:
                            with service as s:
                                s['/Ac/L1/Voltage'] = d["avoltage"]
                                s['/Ac/L1/Current'] = d["acurrent"]
                                s['/Ac/L1/Power'] = d["apower"]
                                s['/Ac/Power'] = d["apower"]
                                avoltage = d.get("avoltage")
                                if avoltage is not None:
                                   s['/Ac/L1/Voltage'] = avoltage
                                acurrent = d.get("acurrent")
                                if acurrent is not None:
                                    s['/Ac/L1/Current'] = acurrent
                                apower = d.get("apower")
                                if apower is not None:
                                    s['/Ac/L1/Power'] = apower
                                    s['/Ac/Power'] = apower
                                
                else:
                    await self.pmSetup()
                    await self.update(data)

    def role_instance(self, value):
        val = value.split(':')
        return val[0], int(val[1])

    def settings_changed(self, service, values):
        if service.alias("instance") in values:
            self.destroy()

    def role_changed(self, val):
        if val not in ['grid', 'pvinverter', 'genset', 'acload']:
            return False

        settings = self.get_settings()
        if settings is None:
            return False

        p = settings.alias("instance")
        role, instance = self.role_instance(settings.get_value(p))
        settings.set_value(p, "{}:{}".format(val, instance))
        self.destroy()  # restart
        return True

    def position_changed(self, val):
        if not 0 <= val <= 2:
            return False

        settings = self.get_settings()
        if settings is None:
            return False

        settings.set_value(settings.alias("position"), val)
        return True
