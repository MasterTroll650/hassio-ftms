"""The FTMS integration."""

import asyncio
import logging

import pyftms
from bleak.exc import BleakError
from homeassistant.components import bluetooth
from homeassistant.components.bluetooth.match import BluetoothCallbackMatcher
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    CONF_ADDRESS,
    CONF_SENSORS,
    EVENT_HOMEASSISTANT_STOP,
    Platform,
)
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr

from .const import DOMAIN
from .coordinator import DataCoordinator
from .models import FtmsData

PLATFORMS: list[Platform] = [
    Platform.BUTTON,
    Platform.NUMBER,
    Platform.SENSOR,
    Platform.SWITCH,
]

_LOGGER = logging.getLogger(__name__)

type FtmsConfigEntry = ConfigEntry[FtmsData]


async def async_unload_entry(hass: HomeAssistant, entry: FtmsConfigEntry) -> bool:
    """Unload a config entry."""

    if not hasattr(entry, "runtime_data"):
        return True

    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        await entry.runtime_data.ftms.disconnect()
        bluetooth.async_rediscover_address(hass, entry.runtime_data.ftms.address)

    return unload_ok


async def async_setup_entry(hass: HomeAssistant, entry: FtmsConfigEntry) -> bool:
    """Load immediately and initialise the machine when it becomes available."""
    task = None
    stopped = False

    async def _initialise() -> None:
        while not stopped:
            try:
                await _async_setup_connected(hass, entry)
                return
            except (ConfigEntryNotReady, BleakError, TimeoutError) as exc:
                _LOGGER.debug("FTMS device not ready; retrying: %s", exc)
            await asyncio.sleep(30)

    @callback
    def _stop_initialise() -> None:
        nonlocal stopped
        stopped = True
        if task and not task.done():
            task.cancel()

    entry.async_on_unload(_stop_initialise)
    task = hass.async_create_background_task(_initialise(), f"ftms setup {entry.entry_id}")
    return True


async def _async_setup_connected(hass: HomeAssistant, entry: FtmsConfigEntry) -> None:
    """Initialise platforms once the machine can be connected."""

    address: str = entry.data[CONF_ADDRESS]

    if not (srv_info := bluetooth.async_last_service_info(hass, address)):
        raise ConfigEntryNotReady(translation_key="device_not_found")

    reconnect_task = None
    stopping = False

    async def _reconnect() -> None:
        """Retry in the background until the machine wakes or is disabled."""
        while not stopping and ftms.need_connect and not ftms.is_connected:
            await asyncio.sleep(15)
            if stopping or not ftms.need_connect or ftms.is_connected:
                break
            try:
                await ftms.connect()
            except Exception as exc:
                _LOGGER.debug("FTMS reconnect failed for %s: %s", address, exc)
            finally:
                coordinator.async_update_listeners()

    def _on_disconnect(ftms_: pyftms.FitnessMachine) -> None:
        """Keep the entry loaded when the machine sleeps."""
        nonlocal reconnect_task
        coordinator.async_update_listeners()
        if not stopping and ftms_.need_connect and (reconnect_task is None or reconnect_task.done()):
            reconnect_task = hass.async_create_task(_reconnect())

    try:
        ftms = pyftms.get_client(
            srv_info.device,
            srv_info.advertisement,
            on_disconnect=_on_disconnect,
        )

    except pyftms.NotFitnessMachineError:
        raise ConfigEntryNotReady(translation_key="ftms_error")

    coordinator = DataCoordinator(hass, ftms)

    try:
        await ftms.connect()

    except BleakError as exc:
        raise ConfigEntryNotReady(translation_key="connection_failed") from exc

    assert ftms.machine_type.name

    _LOGGER.debug(f"Device Information: {ftms.device_info}")
    _LOGGER.debug(f"Machine type: {ftms.machine_type.name}")
    _LOGGER.debug(f"Available sensors: {ftms.available_properties}")
    _LOGGER.debug(f"Supported settings: {ftms.supported_settings}")
    _LOGGER.debug(f"Supported ranges: {ftms.supported_ranges}")

    unique_id = "".join(
        x for x in ftms.device_info.get("serial_number", address) if x.isalnum()
    ).lower()

    _LOGGER.debug(f"Registered new FTMS device. UniqueID is '{unique_id}'.")

    device_info = dr.DeviceInfo(
        connections={(dr.CONNECTION_BLUETOOTH, ftms.address)},
        identifiers={(DOMAIN, unique_id)},
        translation_key=ftms.machine_type.name.lower(),
        **ftms.device_info,
    )

    entry.runtime_data = FtmsData(
        entry_id=entry.entry_id,
        unique_id=unique_id,
        device_info=device_info,
        ftms=ftms,
        coordinator=coordinator,
        sensors=entry.options[CONF_SENSORS],
    )

    @callback
    def _async_on_ble_event(
        srv_info: bluetooth.BluetoothServiceInfoBleak,
        change: bluetooth.BluetoothChange,
    ) -> None:
        """Update from a ble callback."""

        ftms.set_ble_device_and_advertisement_data(
            srv_info.device, srv_info.advertisement
        )

    entry.async_on_unload(
        bluetooth.async_register_callback(
            hass,
            _async_on_ble_event,
            BluetoothCallbackMatcher(address=address),
            bluetooth.BluetoothScanningMode.ACTIVE,
        )
    )

    @callback
    def _stop_reconnect() -> None:
        nonlocal stopping
        stopping = True
        if reconnect_task is not None:
            reconnect_task.cancel()

    entry.async_on_unload(_stop_reconnect)

    # Platforms initialization
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    entry.async_on_unload(entry.add_update_listener(_async_entry_update_handler))

    async def _async_hass_stop_handler(event: Event) -> None:
        """Close the connection."""

        await ftms.disconnect()

    entry.async_on_unload(
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_hass_stop_handler)
    )

    return True


async def _async_entry_update_handler(
    hass: HomeAssistant, entry: FtmsConfigEntry
) -> None:
    """Options update handler."""

    if entry.options[CONF_SENSORS] != entry.runtime_data.sensors:
        hass.config_entries.async_schedule_reload(entry.entry_id)
