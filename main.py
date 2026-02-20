import os

# The decky plugin module is located at decky-loader/plugin
# For easy intellisense checkout the decky-loader code repo
# and add the `decky-loader/plugin/imports` path to `python.analysis.extraPaths` in `.vscode/settings.json`
import decky
import asyncio


class Plugin:
    # Diagnostic method for frontend/backend connectivity checks.
    async def add(self, left: int, right: int) -> int:
        return left + right

    async def long_running(self):
        await asyncio.sleep(15)
        await decky.emit("timer_event", "Hello from pw-ac3-live backend", True, 2)

    # Asyncio-compatible long-running code, executed in a task when the plugin is loaded
    async def _main(self):
        self.loop = asyncio.get_event_loop()
        decky.logger.info("pw-ac3-live backend loaded")

    # Function called first during the unload process, utilize this to handle your plugin being stopped, but not
    # completely removed
    async def _unload(self):
        decky.logger.info("pw-ac3-live backend unloading")

    # Function called after `_unload` during uninstall, utilize this to clean up processes and other remnants of your
    # plugin that may remain on the system
    async def _uninstall(self):
        decky.logger.info("pw-ac3-live backend uninstall")

    async def start_timer(self):
        self.loop.create_task(self.long_running())

    # Migrations that should be performed before entering `_main()`.
    async def _migration(self):
        decky.logger.info("Running migrations for decky-pw-ac3-live")

        decky.migrate_logs(
            os.path.join(
                decky.DECKY_USER_HOME,
                ".config",
                "decky-pw-ac3-live",
                "plugin.log",
            )
        )
        decky.migrate_settings(
            os.path.join(decky.DECKY_HOME, "settings", "decky-pw-ac3-live.json"),
            os.path.join(decky.DECKY_USER_HOME, ".config", "decky-pw-ac3-live"),
        )
        decky.migrate_runtime(
            os.path.join(decky.DECKY_HOME, "decky-pw-ac3-live"),
            os.path.join(decky.DECKY_USER_HOME, ".local", "share", "decky-pw-ac3-live"),
        )
