# SuggestBounties Cog

A Discord bot cog that automatically converts approved suggestions to GitHub issues as bounties, with comprehensive tracking and retry capabilities.

## Features

- **Automatic Bounty Creation**: Converts approved suggestions to GitHub issues using customizable templates
- **Comprehensive Tracking**: Tracks all suggestions and their GitHub issue creation status
- **Retry System**: Retry failed bounty creation for individual suggestions or all at once
- **Status Monitoring**: Check the status of suggestions and bounty creation
- **Flexible Configuration**: Customizable GitHub issue templates and requirements

## Commands

### Configuration Commands
- `suggestbountyset repo <owner/repo>` - Set GitHub repository
- `suggestbountyset token <token>` - Set GitHub personal access token
- `suggestbountyset channel <#channel>` - Set suggestion channel
- `suggestbountyset schema` - Upload GitHub issue template YAML
- `suggestbountyset show` - Show current configuration
- `suggestbountyset reasonflag [keyword]` - Set/clear reason requirement
- `suggestbountyset toggle` - Toggle automatic suggestion processing

### Retry & Status Commands
- `retrybounty [number]` - Retry failed bounty creation for a specific suggestion
- `retryallbounties` - Retry all failed bounties at once
- `bountystatus [number]` - Check bounty creation status
- `clearbountytracking [number]` - Clear tracking data
- `syncbounties` - Sync existing bounties from channel reactions
- `bountyhelp` - Show help for all commands

## Retry System

The retry system addresses the issue where some bounties fail to be created in GitHub at the time of accepting suggestions. It provides several ways to retry failed bounty creation:

## Sync System

The sync system allows you to populate the tracking system with existing bounties that were created before the tracking was implemented:

### Manual Sync
```
[p]syncbounties
```
Scans the suggestion channel for existing posts and checks the bot's reactions to determine their status:
- ✅ reaction = Successfully created GitHub issue
- ❌ reaction = Failed to create GitHub issue  
- No reaction = Pending/unknown status

The sync command will:
1. Scan the last 100 messages in the suggestion channel
2. Identify approved suggestion posts
3. Check bot reactions to determine bounty status
4. Attempt to find corresponding GitHub issues for successful bounties
5. Populate the tracking system with all found suggestions

### GitHub Issue Matching
When syncing successful bounties, the system attempts to find the corresponding GitHub issue using:
- Suggestion number in issue title or body
- Similar title text matching
- Repository search queries
- Best-effort heuristics for matching

This ensures that even manually created GitHub issues can be linked back to Discord suggestions.

### Individual Retry
```
[p]retrybounty 123
```
Retries creating a GitHub bounty for suggestion #123.

### List Failed Suggestions
```
[p]retrybounty
```
Shows all failed suggestions that can be retried.

### Bulk Retry
```
[p]retryallbounties
```
Attempts to retry all failed suggestions at once, providing a summary of results.

### Status Checking
```
[p]bountystatus
```
Shows overview of all tracked suggestions.

```
[p]bountystatus 123
```
Shows detailed status for suggestion #123.

## How It Works

1. **Automatic Detection**: The cog monitors the configured suggestion channel for approved suggestions
2. **Bounty Creation**: When a suggestion is approved, it automatically creates a GitHub issue
3. **Status Tracking**: All suggestions are tracked with their creation status
4. **Retry Capability**: Failed suggestions can be manually retried using the retry commands
5. **Reaction Updates**: Original suggestion messages are updated with ✅ (success) or ❌ (failed) reactions

## Setup

1. **Configure GitHub Repository**:
   ```
   [p]suggestbountyset repo owner/repo
   ```

2. **Set GitHub Token**:
   ```
   [p]suggestbountyset token your_github_token
   ```

3. **Set Suggestion Channel**:
   ```
   [p]suggestbountyset channel #suggestions
   ```

4. **Upload Issue Template** (optional):
   ```
   [p]suggestbountyset schema
   ```

5. **Enable Auto-Processing**:
   ```
   [p]suggestbountyset toggle
   ```

## Requirements

- `PyGithub` - For GitHub API integration
- `requests` - For HTTP requests
- Valid GitHub personal access token
- Discord bot with admin permissions

## Use Cases

- **Game Development**: Convert player suggestions to development tasks
- **Community Projects**: Track feature requests and bug reports
- **Open Source**: Manage community contributions and issues
- **Project Management**: Streamline suggestion-to-task workflow

## Troubleshooting

### Failed Bounty Creation
If bounties fail to create automatically:
1. Check the bot logs for error messages
2. Verify GitHub token permissions
3. Ensure repository exists and is accessible
4. Use `[p]retrybounty` to retry failed suggestions

### Missing Suggestions
If suggestions aren't being tracked:
1. Verify the suggestion channel is set correctly
2. Check if auto-processing is enabled
3. Ensure suggestions follow the expected format
4. Use `[p]bountystatus` to check tracking status

## Support

For issues or questions:
1. Check the bot logs for error messages
2. Verify all configuration settings
3. Ensure GitHub token has appropriate permissions
4. Use `[p]bountyhelp` for command reference
