"""
Default configurations for the SS13Verify/CkeyTools cog.
This module defines all default configuration values used across different modules.
"""

# Default guild configuration
DEFAULT_GUILD_CONFIG = {
    # Verification system settings
    "ticket_channel": None,  # Channel ID for ticket panel
    "ticket_category": None,  # Category ID for ticket channels
    "panel_message": None,   # Message ID for the panel embed
    "panel_embed": {},       # JSON dict for the panel embed
    "ticket_embed": {},      # JSON dict for the ticket embed
    "verification_roles": [], # List of role IDs to assign on verification

    # Database settings
    "db_host": "127.0.0.1",
    "db_port": 3306,
    "db_user": None,
    "db_password": None,
    "db_name": None,
    "mysql_prefix": "",

    # System toggles
    "invalidate_on_leave": False,  # Whether to invalidate verification when user leaves
    "verification_enabled": False,  # Whether verification system is enabled
    "autoverification_enabled": False,  # Whether auto-verification is enabled
    "autoverify_on_join_enabled": False,  # Whether auto-verification on join is enabled
    "deverified_users": [],  # List of user IDs who have been manually deverified

    # Ticket permission system
    "ticket_default_permissions": {},  # Default permissions for @everyone in tickets
    "ticket_staff_roles": [],  # List of role IDs that get staff access to tickets
    "ticket_staff_permissions": {},  # Permissions for staff roles in tickets
    "ticket_opener_permissions": {},  # Permissions for the user who opened the ticket

    # Autodonator settings
    "autodonator_enabled": False,  # Whether autodonator system is enabled
    "config_folder": None,  # Folder path for donator files
    "donator_tiers": {},  # Dynamic tier configuration: {"tier_path": [role_ids]}
}

# Default member configuration
DEFAULT_MEMBER_CONFIG = {
    "open_ticket": None,  # Channel ID of open ticket, if any
}

# Default role configuration (for autodonator)
DEFAULT_ROLE_CONFIG = {
    "donator_tier_path": None,  # Path like "donators/tier_1" for this role
}

# Autodonator tier configuration examples
EXAMPLE_TIER_CONFIGS = {
    "donators/tier_1": [],
    "donators/tier_2": [],
    "donators/tier_3": [],
    "supporters/bronze": [],
    "supporters/silver": [],
    "supporters/gold": [],
    "vip/standard": [],
    "vip/premium": [],
}
