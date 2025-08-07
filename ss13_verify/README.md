# CkeyTools - SS13 Discord Verification & Management System

A comprehensive SS13 Discord verification and management system with modular design, featuring ticket-based verification, auto-verification, role assignment, database linking, and flexible autodonator functionality.

## Features

### 🔐 Discord Verification System
- **Ticket-based verification**: Users click a panel button to create private verification tickets
- **Auto-verification**: Returning users are automatically re-verified based on previous links
- **Manual verification**: Support for one-time verification codes from the game
- **Persistent UI**: Buttons and modals work even after bot restarts
- **Permission system**: Granular control over ticket permissions for staff, users, and @everyone

### 🗄️ Database Integration
- **SQLAlchemy ORM**: Modern async database operations with connection pooling
- **MySQL/MariaDB support**: Compatible with SS13 server databases
- **Guild-specific connections**: Each Discord server can have its own database
- **Transaction safety**: Proper error handling and rollback mechanisms

### 🎁 Flexible Autodonator System
- **Dynamic tier naming**: Support for any tier structure (e.g., `donators/tier_1`, `supporters/bronze`, `vip/premium/gold`)
- **TOML file generation**: Automatic creation of configuration files for SS13 servers
- **Role-based management**: Assign multiple roles to any tier
- **Auto-updates**: Files are regenerated when roles change (every 5 minutes)

### ⚙️ Modular Architecture
- **Separate modules**: Verification and autodonator functionality in separate files
- **Mixin pattern**: Clean separation of concerns using Python mixins
- **Common utilities**: Shared configurations and helper functions
- **Easy extension**: Simple to add new modules and features

## Installation

1. Add the repository to your Red instance:
```
[p]repo add mal0-cogs https://github.com/Mal0/Mal0-cogs
```

2. Install the cog:
```
[p]cog install mal0-cogs ss13_verify
```

3. Load the cog:
```
[p]load ss13_verify
```

## Configuration

### Database Setup

First, configure your database connection:

```
[p]ckeytools settings database host localhost
[p]ckeytools settings database port 3306
[p]ckeytools settings database user your_username
[p]ckeytools settings database password your_password
[p]ckeytools settings database name your_database
[p]ckeytools settings database prefix ss13_  # Optional table prefix
[p]ckeytools settings database reconnect
```

### Verification Panel Setup

1. **Set channels**:
```
[p]ckeytools settings panel setchannel #verification-panel
[p]ckeytools settings panel setcategory "Verification Tickets"
```

2. **Configure embeds** (attach JSON files):
```
[p]ckeytools settings panel setembed
[p]ckeytools settings panel setticketembed
```

3. **Add verification roles**:
```
[p]ckeytools settings roles add @Verified
[p]ckeytools settings roles add @Player
```

4. **Create the panel**:
```
[p]ckeytools settings panel create
```

### System Toggles

Enable the systems you want to use:

```
[p]ckeytools settings verification true
[p]ckeytools settings autoverification true
[p]ckeytools settings autoverifyonjoin true
[p]ckeytools settings invalidateonleave true  # "Force stay" functionality
```

### Autodonator Setup

1. **Enable autodonator**:
```
[p]ckeytools autodonator toggle true
```

2. **Set output folder**:
```
[p]ckeytools autodonator folder /path/to/your/ss13/config
```

3. **Configure tiers** (flexible naming):
```
[p]ckeytools autodonator addtier donators/tier_1 @Tier1Donator
[p]ckeytools autodonator addtier donators/tier_2 @Tier2Donator
[p]ckeytools autodonator addtier supporters/bronze @BronzeSupporter
[p]ckeytools autodonator addtier supporters/silver @SilverSupporter
[p]ckeytools autodonator addtier vip/premium/gold @GoldVIP
```

4. **Manual update** (optional):
```
[p]ckeytools autodonator update
```

## Commands

### Main Commands

- `[p]ckeytools` - Main command group
- `[p]ckeytools status` - Show system status and configuration
- `[p]ckeytools checkuser <user>` - Check a user's verification status
- `[p]ckeytools ckeys <user>` - List all historical ckeys for a Discord user
- `[p]ckeytools discords <ckey>` - List all historical Discord accounts for a ckey
- `[p]deverify [user]` - Deverify yourself or another user (kicks from server)

### Settings Commands

#### Database Configuration
- `[p]ckeytools settings database host <host>` - Set database host
- `[p]ckeytools settings database port <port>` - Set database port
- `[p]ckeytools settings database user <user>` - Set database username
- `[p]ckeytools settings database password <password>` - Set database password
- `[p]ckeytools settings database name <name>` - Set database name
- `[p]ckeytools settings database prefix <prefix>` - Set table prefix
- `[p]ckeytools settings database reconnect` - Test database connection

#### Role Management
- `[p]ckeytools settings roles add <role>` - Add verification role
- `[p]ckeytools settings roles remove <role>` - Remove verification role
- `[p]ckeytools settings roles list` - List verification roles
- `[p]ckeytools settings roles clear` - Clear all verification roles

#### Panel Configuration
- `[p]ckeytools settings panel setchannel <channel>` - Set panel channel
- `[p]ckeytools settings panel setcategory <category>` - Set ticket category
- `[p]ckeytools settings panel setembed` - Set panel embed (attach JSON)
- `[p]ckeytools settings panel setticketembed` - Set ticket embed (attach JSON)
- `[p]ckeytools settings panel create` - Create verification panel

#### System Toggles
- `[p]ckeytools settings verification <true/false>` - Toggle verification system
- `[p]ckeytools settings autoverification <true/false>` - Toggle auto-verification
- `[p]ckeytools settings autoverifyonjoin <true/false>` - Toggle auto-verify on join
- `[p]ckeytools settings invalidateonleave <true/false>` - Toggle invalidate on leave

### Autodonator Commands

- `[p]ckeytools autodonator toggle <true/false>` - Enable/disable autodonator
- `[p]ckeytools autodonator folder <path>` - Set output folder
- `[p]ckeytools autodonator addtier <tier_path> <role>` - Add role to tier
- `[p]ckeytools autodonator removetier <tier_path> <role>` - Remove role from tier
- `[p]ckeytools autodonator listtiers` - List all configured tiers
- `[p]ckeytools autodonator update` - Manually update TOML file
- `[p]ckeytools autodonator preview` - Preview current TOML content

## File Structure

```
ss13_verify/
├── __init__.py                 # Cog initialization
├── ckeytools.py               # Main cog class (combines all mixins)
├── core_commands.py           # Core commands (status, main group)
├── info.json                  # Cog metadata
├── database.py                # Database manager (SQLAlchemy)
├── models.py                  # Database models (DiscordLink)
├── helpers.py                 # Utility functions
├── common/                    # Shared utilities and configurations
│   ├── __init__.py
│   └── config.py             # Default configuration values
├── verify/                    # Discord verification module
│   ├── __init__.py
│   ├── verify_mixin.py       # Verification functionality (non-command methods)
│   ├── commands.py           # Verification commands mixin
│   └── ui_components.py      # Discord UI components (buttons, modals)
└── autodonator/              # Autodonator module
    ├── __init__.py
    ├── autodonator_mixin.py  # Autodonator functionality (non-command methods)
    └── commands.py           # Autodonator commands mixin
```

## Autodonator Tier Examples

The flexible tier system supports any naming structure:

### Traditional Tiers
```toml
[donators]
tier_1 = ["ckey1", "ckey2"]
tier_2 = ["ckey3", "ckey4"]
tier_3 = ["ckey5", "ckey6"]
```

### Supporter Levels
```toml
[supporters]
bronze = ["ckey1", "ckey2"]
silver = ["ckey3", "ckey4"]
gold = ["ckey5", "ckey6"]
```

### Complex Hierarchies
```toml
[vip]
[vip.standard]
basic = ["ckey1", "ckey2"]
premium = ["ckey3", "ckey4"]

[vip.premium]
gold = ["ckey5", "ckey6"]
platinum = ["ckey7", "ckey8"]
```

## Database Schema

The cog uses the following database table structure:

```sql
CREATE TABLE `discord_links` (
    `id` INT(11) NOT NULL AUTO_INCREMENT,
    `ckey` VARCHAR(32) NOT NULL,
    `discord_id` BIGINT(20) NULL DEFAULT NULL,
    `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    `one_time_token` VARCHAR(100) NOT NULL,
    `valid` TINYINT(1) NOT NULL DEFAULT '0',
    PRIMARY KEY (`id`),
    INDEX `ckey` (`ckey`),
    INDEX `discord_id` (`discord_id`),
    INDEX `timestamp` (`timestamp`),
    INDEX `one_time_token` (`one_time_token`),
    INDEX `valid` (`valid`)
) COLLATE='utf8mb4_unicode_ci' ENGINE=InnoDB;
```

## Migration from Original Cogs

This cog replaces the following original cogs:
- **tgverify**: Discord verification functionality
- **tgdb**: Database management
- **ckeytools**: Autodonator and force-stay features

### Key Improvements
- **True modular architecture**: Commands and functionality properly separated into mixins
- **Command organization**: Each module contains its own command groups and functionality
- **Proper mixin inheritance**: Main cog combines all mixins using Python's multiple inheritance
- Modern SQLAlchemy ORM instead of raw SQL
- Persistent Discord UI components
- Flexible autodonator tier naming
- Comprehensive permission system for tickets
- Better error handling and logging

### Modular Architecture Benefits
- **Separation of concerns**: Verification, autodonator, and core functionality are in separate modules
- **Easy maintenance**: Changes to one system don't affect others
- **Command organization**: Commands are defined in their respective modules, not the main file
- **Extensible design**: New modules can be easily added by creating new mixin classes
- **Clean inheritance**: Main cog inherits from all mixins to provide complete functionality

## Support

If you encounter any issues or have questions:
1. Check the bot's logs for error messages
2. Verify your database connection and permissions
3. Ensure all required Python packages are installed
4. Check that your Discord bot has the necessary permissions

## License

This project is licensed under the MIT License - see the LICENSE file for details.
