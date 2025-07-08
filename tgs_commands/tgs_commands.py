import aiohttp
import asyncio
import base64
import logging
import os
from redbot.core import commands, Config
from redbot.core.commands import Context
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import humanize_list
from discord.ext import tasks
import re

class TGSCommands(commands.Cog):
	"""
	A cog to interact with the TGS API.
	"""

	__author__ = "Mosley"
	__version__ = "1.0.0"

	def __init__(self, bot):
		self.bot = bot
		# Static headers
		self.api_header = "Tgstation.Server.Api/10.13.0"
		self.user_agent = "Red-TGS-Bot/1.0.0"
		# Bearer token and expiry
		self.bearer_token = None
		self.token_expiry = None
		# Redbot config
		self.config = Config.get_conf(self, identifier=926233194443964436, force_registration=True)
		default_guild = {
			"tgs_url": "https://your-tgs-url.example.com",
			"tgs_username": "your_username",
			"tgs_password": "your_password",
			"savefile_folder": None
		}
		self.config.register_global(**default_guild)
		# Logger
		self.log = logging.getLogger("red.tgs_commands")

	async def cog_load(self):
		# Start the authentication refresh task
		self.refresh_auth.start()
		await self.authenticate()

	def cog_unload(self):
		# Stop the authentication refresh task when unloading
		self.refresh_auth.cancel()

	@tasks.loop(minutes=5.0)
	async def refresh_auth(self):
		"""Task to refresh TGS authentication every 5 minutes."""
		self.log.debug("Running scheduled authentication refresh...")
		await self.authenticate()

	@refresh_auth.before_loop
	async def before_refresh_auth(self):
		"""Wait until the bot is ready before starting the task."""
		await self.bot.wait_until_ready()

	async def authenticate(self):
		conf = await self.config.all()
		tgs_url = conf["tgs_url"].rstrip("/")
		username = conf["tgs_username"]
		password = conf["tgs_password"]
		if not all([tgs_url, username, password]):
			self.log.error("TGS config incomplete. Please set tgs_url, tgs_username, and tgs_password.")
			self.bearer_token = None
			self.token_expiry = None
			return False
		credentials = f"{username}:{password}"
		b64_credentials = base64.b64encode(credentials.encode()).decode()
		headers = {
			"Authorization": f"Basic {b64_credentials}",
			"Api": self.api_header,
			"User-Agent": self.user_agent,
			"Accept": "application/json"
		}
		try:
			async with aiohttp.ClientSession() as session:
				async with session.post(f"{tgs_url}/", headers=headers) as resp:
					if resp.status == 200:
						data = await resp.json()
						self.bearer_token = data.get("bearer")
						# Optionally parse expiry from JWT if needed
						self.token_expiry = None  # Set if you parse expiry
						#self.log.info("TGS authentication successful.")
						return True
					else:
						self.log.error(f"TGS authentication failed: {resp.status} {await resp.text()}")
						self.bearer_token = None
						self.token_expiry = None
						return False
		except Exception as e:
			self.log.error(f"TGS authentication error: {e}")
			self.bearer_token = None
			self.token_expiry = None
			return False

	async def _tgs_request(self, method: str, path: str, extra_headers: dict = None, data=None):
		"""
		Common function to send HTTP requests to TGS API with proper headers.
		Returns (status, data) tuple.
		"""
		conf = await self.config.all()
		tgs_url = conf["tgs_url"].rstrip("/")
		url = f"{tgs_url}{path}"
		headers = {
			"Authorization": f"Bearer {self.bearer_token}",
			"Api": self.api_header,
			"User-Agent": self.user_agent,
			"Accept": "application/json"
		}
		if extra_headers:
			headers.update(extra_headers)
		try:
			async with aiohttp.ClientSession() as session:
				req_args = {"headers": headers}
				if data is not None:
					req_args["json"] = data
				async with session.request(method, url, **req_args) as resp:
					try:
						resp_data = await resp.json(content_type=None)
					except Exception:
						resp_data = await resp.text()
					return resp.status, resp_data
		except Exception as e:
			return None, str(e)

	@commands.group()
	@commands.admin()
	async def tgs(self, ctx: Context):
		"""TGStation Server API commands."""
		pass

	@tgs.group()
	@commands.is_owner()
	async def config(self, ctx: Context):
		"""Configure TGS API connection (owner only)."""
		pass

	@config.command()
	async def seturl(self, ctx: Context, url: str):
		"""Set the TGS API URL."""
		await self.config.tgs_url.set(url)
		await ctx.send(f"TGS URL set to: {url}")
		await ctx.tick()

	@config.command()
	async def setusername(self, ctx: Context, username: str):
		"""Set the TGS API username."""
		await self.config.tgs_username.set(username)
		await ctx.send(f"TGS username set to: {username}")
		await ctx.tick()

	@config.command()
	async def setpassword(self, ctx: Context, password: str):
		"""Set the TGS API password."""
		try:
			await ctx.message.delete()
		except Exception:
			pass
		await self.config.tgs_password.set(password)
		confirm_msg = await ctx.send("TGS password set.")
		await asyncio.sleep(1)
		try:
			await confirm_msg.delete()
		except Exception:
			pass

	@config.command(name="auth")
	async def config_auth(self, ctx: Context):
		"""Attempt to authenticate with the current config."""
		result = await self.authenticate()
		if result:
			await ctx.send("TGS authentication successful.")
			await ctx.tick()
		else:
			await ctx.send("TGS authentication failed. Check logs for details.")

	@config.command(name="savefilefolder")
	async def setsavefilefolder(self, ctx: Context, folder: str):
		"""Set the savefile folder (must be a real folder on the machine)."""
		folder = os.path.abspath(folder)
		if not (os.path.exists(folder) and os.path.isdir(folder)):
			await ctx.send(f"This path is not a valid folder: {folder}")
			return
		await self.config.savefile_folder.set(folder)
		await ctx.send(f"Savefile folder set to: {folder}")
		await ctx.tick()

	@tgs.group()
	async def instances(self, ctx: Context):
		"""Manage TGS instances."""
		pass

	@instances.command(name="list")
	async def instances_list(self, ctx: Context):
		"""List all available TGS instances and their information."""
		if not self.bearer_token:
			await ctx.send("Not authenticated with TGS. Please authenticate first.")
			return
		status, data = await self._tgs_request("GET", "/Instance/List")
		if status == 200:
			instances = data.get("content", []) if isinstance(data, dict) else []
			if not instances:
				await ctx.send("No instances found.")
				await ctx.tick()
				return
			msg = "**TGS Instances:**\n"
			for inst in instances:
				msg += f"\nID: `{inst.get('id', 'N/A')}`\nName: `{inst.get('name', 'N/A')}`\nPath: `{inst.get('path', 'N/A')}`\nOnline: `{inst.get('online', 'N/A')}`\n"
			await ctx.send(msg)
			await ctx.tick()
		elif status is not None:
			await ctx.send(f"Failed to fetch instances: {status} {data}")
		else:
			await ctx.send(f"Error fetching instances: {data}")

	@instances.command(name="restart")
	async def instances_restart(self, ctx: Context, instance_id: int):
		"""Restart the Watchdog for a given instance ID."""
		if not self.bearer_token:
			await ctx.send("Not authenticated with TGS. Please authenticate first.")
			return
		status, data = await self._tgs_request(
			"PATCH", "/DreamDaemon", extra_headers={"Instance": str(instance_id)}
		)
		if status == 202:
			job_id = data.get("id", "N/A") if isinstance(data, dict) else "N/A"
			await ctx.send(f"Restart job started for instance `{instance_id}`. Job ID: `{job_id}`.")
			await ctx.tick()
		elif status is not None:
			await ctx.send(f"Failed to restart instance {instance_id}: {status} {data}")
		else:
			await ctx.send(f"Error restarting instance: {data}")

	@instances.command(name="launch")
	async def instances_launch(self, ctx: Context, instance_id: int):
		"""Launch the Watchdog for a given instance ID."""
		if not self.bearer_token:
			await ctx.send("Not authenticated with TGS. Please authenticate first.")
			return
		status, data = await self._tgs_request(
			"PUT", "/DreamDaemon", extra_headers={"Instance": str(instance_id)}
		)
		if status == 202:
			job_id = data.get("id", "N/A") if isinstance(data, dict) else "N/A"
			await ctx.send(f"Launch job started for instance `{instance_id}`. Job ID: `{job_id}`.")
			await ctx.tick()
		elif status is not None:
			await ctx.send(f"Failed to launch instance {instance_id}: {status} {data}")
		else:
			await ctx.send(f"Error launching instance: {data}")

	@instances.command(name="status")
	async def instances_status(self, ctx: Context, instance_id: int):
		"""Get the Watchdog status for a given instance ID."""
		if not self.bearer_token:
			await ctx.send("Not authenticated with TGS. Please authenticate first.")
			return
		status, data = await self._tgs_request(
			"GET", "/DreamDaemon", extra_headers={"Instance": str(instance_id)}
		)
		if status == 200:
			msg = f"**Watchdog Status for Instance `{instance_id}`:**\n"
			if isinstance(data, dict):
				msg += f"Online: `{data.get('online', 'N/A')}`\n"
				msg += f"Port: `{data.get('port', 'N/A')}`\n"
				msg += f"Current State: `{data.get('rebootState', 'N/A')}`\n"
			else:
				msg += str(data)
			await ctx.send(msg)
			await ctx.tick()
		elif status is not None:
			await ctx.send(f"Failed to get status for instance {instance_id}: {status} {data}")
		else:
			await ctx.send(f"Error getting status: {data}")

	@instances.command(name="stop")
	async def instances_stop(self, ctx: Context, instance_id: int):
		"""Stop the Watchdog for a given instance ID."""
		if not self.bearer_token:
			await ctx.send("Not authenticated with TGS. Please authenticate first.")
			return
		status, data = await self._tgs_request(
			"DELETE", "/DreamDaemon", extra_headers={"Instance": str(instance_id)}
		)
		if status == 204:
			await ctx.send(f"Watchdog stopped for instance `{instance_id}`.")
			await ctx.tick()
		elif status is not None:
			await ctx.send(f"Failed to stop instance {instance_id}: {status} {data}")
		else:
			await ctx.send(f"Error stopping instance: {data}")

	@tgs.group()
	async def savefile(self, ctx: Context):
		"""Manage SS13 savefiles."""
		pass

	@savefile.command(name="import")
	async def savefile_import(self, ctx: Context):
		"""Import a preferences JSON file for a ckey. Expects a file named '[ckey]_preferences_[date_and_time].json'."""
		if not ctx.message.attachments:
			await ctx.send("You must attach a JSON file with the correct name format.")
			return
		attachment = ctx.message.attachments[0]
		filename = attachment.filename
		# Match pattern: [ckey]_preferences_[date_and_time].json
		match = re.match(r"^([a-zA-Z0-9_]+)_preferences_.*\\.json$", filename)
		if not match:
			await ctx.send("Filename must be in the format '[ckey]_preferences_[date_and_time].json'.")
			return
		ckey = match.group(1)
		c = ckey[0].lower()
		savefile_folder = await self.config.savefile_folder()
		if not savefile_folder:
			await ctx.send("Savefile folder is not configured. Please set it with the config command.")
			return
		target_dir = os.path.join(savefile_folder, c, ckey)
		os.makedirs(target_dir, exist_ok=True)
		# Remove old preferences.json and preferences.json.updatebac if they exist
		for fname in ["preferences.json", "preferences.json.updatebac"]:
			fpath = os.path.join(target_dir, fname)
			try:
				if os.path.exists(fpath):
					os.remove(fpath)
			except Exception as e:
				await ctx.send(f"Failed to remove old file {fname}: {e}")
				return
		# Download and save the new file as preferences.json
		dest_path = os.path.join(target_dir, "preferences.json")
		try:
			await attachment.save(dest_path)
			await ctx.send(f"Preferences imported for ckey '{ckey}' at {dest_path}.")
			await ctx.tick()
		except Exception as e:
			await ctx.send(f"Failed to import preferences: {e}")
