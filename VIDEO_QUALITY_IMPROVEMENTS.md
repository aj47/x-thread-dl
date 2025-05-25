# Video Quality Improvements for x-thread-dl

## Overview

This document outlines the improvements made to enhance video download quality in the x-thread-dl tool. Based on research into Twitter/X video formats and yt-dlp best practices, several key enhancements have been implemented.

## Key Improvements

### 1. Enhanced yt-dlp Format Selection

**Before:**
```python
'format': 'best[ext=mp4]/best'
```

**After:**
```python
'format': (
    'bestvideo[height<=2160][ext=mp4]+bestaudio[ext=m4a]/'  # 4K MP4 + M4A audio
    'bestvideo[ext=mp4]+bestaudio[ext=m4a]/'                # Best MP4 + M4A audio  
    'best[ext=mp4]/'                                        # Best single MP4 file
    'best'                                                  # Fallback to any best format
)
```

**Benefits:**
- Supports up to 4K resolution (2160p) as now supported by X/Twitter
- Combines best video and audio streams for optimal quality
- Maintains MP4 compatibility while maximizing quality
- Graceful fallback to ensure downloads always work

### 2. Improved Video Variant Selection

**Enhanced Logic:**
- Prioritizes MP4 format over other video types
- Sorts variants by bitrate in descending order (highest first)
- Adds detailed logging of available variants and selection process
- Better handling of both `video` and `mediaDetails` structures in Twitter API responses

**Code Example:**
```python
# Sort by bitrate if available, highest first for best quality
mp4_variants.sort(key=lambda x: x.get('bitrate', 0), reverse=True)
best_variant = mp4_variants[0]
logger.info(f"Selected MP4 variant with bitrate: {best_variant.get('bitrate', 'unknown')}")
```

### 3. Format Listing and Debugging

**New Features:**
- `list_video_formats()` function to inspect available formats without downloading
- `--list-formats` command-line option to enable format listing
- Detailed logging of video resolution, bitrate, and codec information
- File size reporting after successful downloads

**Usage:**
```bash
python main.py --verbose --list-formats "https://x.com/user/status/123456789"
```

### 4. Enhanced Logging and Monitoring

**Improvements:**
- Log available video variants during extraction
- Report selected format details (resolution, bitrate, codec)
- Show file size after successful downloads
- Better error handling and debugging information

**Example Output:**
```
2024-05-24 19:52:21 - INFO - Selected MP4 variant with bitrate: 4096000
2024-05-24 19:52:22 - INFO - Selected format for 123456789: bestvideo[height<=2160][ext=mp4]+bestaudio[ext=m4a]
2024-05-24 19:52:23 - INFO - Video resolution: 1080p
2024-05-24 19:52:24 - INFO - Total bitrate: 4096 kbps
2024-05-24 19:52:25 - INFO - Successfully downloaded video to output/user/123456789/videos/123456789.mp4 (15.2 MB)
```

## Technical Details

### yt-dlp Format String Explanation

The new format string uses yt-dlp's advanced format selection:

1. `bestvideo[height<=2160][ext=mp4]+bestaudio[ext=m4a]` - Best video up to 4K + best audio
2. `bestvideo[ext=mp4]+bestaudio[ext=m4a]` - Best MP4 video + best M4A audio (any resolution)
3. `best[ext=mp4]` - Best single MP4 file (video+audio combined)
4. `best` - Any best format as fallback

### Video Quality Hierarchy

1. **4K (2160p)** - Highest quality supported by X/Twitter
2. **1080p** - Full HD, commonly available
3. **720p** - HD quality
4. **Lower resolutions** - For bandwidth-constrained scenarios

### Bitrate Considerations

- Higher bitrate = better quality but larger file size
- Twitter typically provides variants ranging from ~300kbps to 4000kbps+
- The tool now automatically selects the highest available bitrate

## Testing

A test script `test_video_quality.py` has been created to:

1. Test format listing functionality
2. Verify enhanced download capabilities
3. Validate format selection logic
4. Demonstrate quality improvements

**Run the test:**
```bash
python test_video_quality.py
```

## Usage Examples

### Basic Usage (with quality improvements)
```bash
python main.py "https://x.com/user/status/123456789"
```

### Verbose mode with format listing
```bash
python main.py --verbose --list-formats "https://x.com/user/status/123456789"
```

### Custom output directory
```bash
python main.py --output-dir ./downloads --list-formats "https://x.com/user/status/123456789"
```

## Expected Quality Improvements

1. **Higher Resolution**: Support for up to 4K videos when available
2. **Better Audio**: Separate audio stream selection for optimal quality
3. **Larger File Sizes**: Higher quality means larger files (expected)
4. **More Reliable Downloads**: Better fallback options ensure downloads succeed
5. **Better Debugging**: Detailed logs help troubleshoot quality issues

## Compatibility

- **Backward Compatible**: All existing functionality preserved
- **Optional Features**: Format listing is opt-in via `--list-formats` flag
- **Graceful Degradation**: Falls back to lower quality if high quality unavailable
- **Cross-Platform**: Works on all platforms supported by yt-dlp

## Future Enhancements

Potential future improvements could include:

1. **Quality Presets**: Allow users to specify preferred quality levels
2. **Bandwidth Optimization**: Automatic quality selection based on connection speed
3. **Format Preferences**: User-configurable format preferences
4. **Parallel Downloads**: Download multiple quality versions simultaneously
5. **Quality Comparison**: Side-by-side quality analysis tools

## Conclusion

These improvements significantly enhance the video download quality while maintaining the tool's reliability and ease of use. Users can now download the highest quality videos available from X/Twitter with detailed visibility into the selection process.
