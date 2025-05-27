"""
Media downloader module for the x-thread-dl tool.
Handles downloading videos from X.com (Twitter) using yt-dlp
and saving text content.
"""

import os
import json
import logging
import yt_dlp
from typing import Optional, Dict, List, Any

# Import configuration and exceptions
import config # To get DEFAULT_OUTPUT_DIR
from exceptions import VideoDownloadError, ValidationError

# Set up logging
logging.basicConfig(
    level=logging.INFO, # Changed to INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('x-thread-dl.media_downloader') # Renamed logger

def _ensure_dir_exists(dir_path: str):
    """Ensure directory exists, creating it if necessary.
    
    Raises:
        VideoDownloadError: If directory creation fails.
    """
    try:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            logger.debug(f"Created directory: {dir_path}")
    except Exception as e:
        raise VideoDownloadError(f"Failed to create directory {dir_path}: {str(e)}") from e

def _save_json_content(data: Dict[str, Any], file_path: str):
    """Saves dictionary data as JSON to the specified file path.
    
    Raises:
        VideoDownloadError: If saving fails.
    """
    try:
        if not data:
            raise ValidationError("No data provided to save")
        if not file_path:
            raise ValidationError("No file path provided")
            
        _ensure_dir_exists(os.path.dirname(file_path))
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"Successfully saved JSON content to {file_path}")
    except (ValidationError, VideoDownloadError):
        # Re-raise our custom exceptions
        raise
    except Exception as e:
        raise VideoDownloadError(f"Error saving JSON content to {file_path}: {str(e)}") from e

def list_video_formats(video_url: str) -> List[Dict[str, Any]]:
    """
    List available video formats for a given URL without downloading.

    Args:
        video_url (str): The URL of the video to inspect.

    Returns:
        List[Dict[str, Any]]: List of available formats
        
    Raises:
        ValidationError: If video_url is invalid.
        VideoDownloadError: If format listing fails.
    """
    try:
        if not video_url or not isinstance(video_url, str):
            raise ValidationError("Video URL must be a non-empty string")
            
        logger.info(f"Listing available formats for: {video_url}")

        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'ignoreerrors': True,
            'listformats': True,
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            if info and 'formats' in info:
                formats = info['formats']
                logger.info(f"Found {len(formats)} available formats")
                for fmt in formats:
                    logger.debug(f"Format: {fmt.get('format_id', 'N/A')} - "
                               f"Quality: {fmt.get('height', 'N/A')}p - "
                               f"Ext: {fmt.get('ext', 'N/A')} - "
                               f"Bitrate: {fmt.get('tbr', 'N/A')} - "
                               f"Codec: {fmt.get('vcodec', 'N/A')}")
                return formats
        
        # If we get here, no formats were found
        raise VideoDownloadError(f"No video formats found for URL: {video_url}")

    except (ValidationError, VideoDownloadError):
        # Re-raise our custom exceptions
        raise
    except Exception as e:
        raise VideoDownloadError(f"Error listing formats for {video_url}: {str(e)}") from e

def download_video_content(video_url: str, video_id: str, output_dir: str, list_formats: bool = False) -> str:
    """
    Download a single video using yt-dlp with enhanced quality options.
    The filename will be {video_id}.mp4.

    Args:
        video_url (str): The URL of the video to download.
        video_id (str): The ID of the tweet/reply containing the video (used for filename).
        output_dir (str): The directory to save the video to.
        list_formats (bool): Whether to list available formats before downloading.

    Returns:
        str: The path to the downloaded video
        
    Raises:
        ValidationError: If input parameters are invalid.
        VideoDownloadError: If download fails.
    """
    try:
        # Validate inputs
        if not video_url or not isinstance(video_url, str):
            raise ValidationError("Video URL must be a non-empty string")
        if not video_id or not isinstance(video_id, str):
            raise ValidationError("Video ID must be a non-empty string")
        if not output_dir or not isinstance(output_dir, str):
            raise ValidationError("Output directory must be a non-empty string")
            
        _ensure_dir_exists(output_dir)

        output_filename = f"{video_id}.mp4"
        output_path = os.path.join(output_dir, output_filename)

        logger.info(f"Downloading video from {video_url} to {output_path}")

        # List formats if requested
        if list_formats:
            try:
                formats = list_video_formats(video_url)
                if formats:
                    logger.info(f"Available formats for video {video_id}:")
                    for fmt in formats[:10]:  # Show first 10 formats
                        logger.info(f"  {fmt.get('format_id', 'N/A')}: {fmt.get('height', 'N/A')}p "
                                  f"{fmt.get('ext', 'N/A')} {fmt.get('tbr', 'N/A')}kbps")
            except Exception as e:
                logger.warning(f"Could not list formats: {e}")

        # Enhanced format selection for better quality
        # Priority: 4K video + audio > best video + audio > best single file
        ydl_opts = {
            'format': (
                'bestvideo[height<=2160][ext=mp4]+bestaudio[ext=m4a]/'  # 4K MP4 + M4A audio
                'bestvideo[ext=mp4]+bestaudio[ext=m4a]/'                # Best MP4 + M4A audio
                'best[ext=mp4]/'                                        # Best single MP4 file
                'best'                                                  # Fallback to any best format
            ),
            'outtmpl': output_path,
            'quiet': False, # Set to False to see yt-dlp output, True for silent
            'no_warnings': False,
            'ignoreerrors': True,
            'nooverwrites': False, # Allow overwriting for retries or updates
            'retries': 5,
            'logger': logger, # Pass our logger to yt-dlp
            'merge_output_format': 'mp4',  # Ensure final output is MP4
            # Consider adding user agent if facing blocks:
            # 'http_headers': {'User-Agent': 'Mozilla/5.0 ...'}
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Extract info to log selected format
            try:
                info = ydl.extract_info(video_url, download=False)
                if info:
                    if 'format' in info:
                        logger.info(f"Selected format for {video_id}: {info.get('format', 'Unknown')}")
                    if 'height' in info:
                        logger.info(f"Video resolution: {info.get('height', 'Unknown')}p")
                    if 'tbr' in info:
                        logger.info(f"Total bitrate: {info.get('tbr', 'Unknown')} kbps")
            except Exception as e:
                logger.debug(f"Could not extract format info: {e}")

            # Download the video
            try:
                ydl.download([video_url])
            except Exception as e:
                raise VideoDownloadError(f"yt-dlp download failed for {video_url}: {str(e)}") from e

        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            file_size = os.path.getsize(output_path) / (1024 * 1024)  # Size in MB
            logger.info(f"Successfully downloaded video to {output_path} ({file_size:.1f} MB)")
            return output_path
        else:
            # Check if a .part file exists, indicating an interrupted download
            part_file = output_path + ".part"
            if os.path.exists(part_file):
                error_msg = f"Partial download file found: {part_file}. Download may have been interrupted."
            else:
                error_msg = f"Video file not found or empty after download attempt: {output_path}"
            raise VideoDownloadError(error_msg)

    except (ValidationError, VideoDownloadError):
        # Re-raise our custom exceptions
        raise
    except Exception as e:
        # Wrap unexpected exceptions
        raise VideoDownloadError(f"Unexpected error downloading video {video_id} from {video_url}: {str(e)}") from e

def save_parsed_thread_data(parsed_data: Dict[str, Any], base_output_dir: str = config.DEFAULT_OUTPUT_DIR, list_formats: bool = False) -> List[str]:
    """
    Saves all parsed thread data (text and videos) according to the structured format:
    output/{user_screen_name}/{thread_id}/thread_text.json
    output/{user_screen_name}/{thread_id}/videos/{tweet_id}.mp4
    output/{user_screen_name}/{thread_id}/replies/{reply_id}/reply_text.json
    output/{user_screen_name}/{thread_id}/replies/{reply_id}/videos/{reply_id}.mp4

    Args:
        parsed_data (Dict[str, Any]): The structured data from thread_parser.
        base_output_dir (str): The base directory for all output (e.g., "output").
        list_formats (bool): Whether to list video formats before downloading.

    Returns:
        List[str]: List of paths to all successfully saved/downloaded files.
        
    Raises:
        ValidationError: If parsed_data is invalid.
        VideoDownloadError: If saving/downloading fails.
    """
    try:
        if not parsed_data:
            raise ValidationError("No parsed data provided to save")
        if not isinstance(parsed_data, dict):
            raise ValidationError("Parsed data must be a dictionary")

        user_screen_name = parsed_data.get("user_screen_name", "unknown_user")
        thread_id = parsed_data.get("thread_id", "unknown_thread")

        # Path for the current thread: output/{user_screen_name}/{thread_id}/
        thread_path = os.path.join(base_output_dir, user_screen_name, thread_id)
        _ensure_dir_exists(thread_path)

        saved_files = []

        # 1. Save main thread text content
        thread_text_content = parsed_data.get("thread_text_content")
        if thread_text_content:
            thread_text_path = os.path.join(thread_path, "thread_text.json")
            _save_json_content(thread_text_content, thread_text_path)
            saved_files.append(thread_text_path)

        # 2. Download main thread videos
        thread_videos_path = os.path.join(thread_path, "videos")
        for video_info in parsed_data.get("thread_videos", []):
            video_url = video_info.get("video_url")
            # Use main thread_id for its videos, as video_info.tweet_id is the same
            video_id_for_filename = thread_id
            if video_url and video_id_for_filename:
                try:
                    downloaded_path = download_video_content(video_url, video_id_for_filename, thread_videos_path, list_formats=list_formats)
                    saved_files.append(downloaded_path)
                except VideoDownloadError as e:
                    logger.error(f"Failed to download thread video: {e}")
                    # Continue processing other videos instead of failing completely

        # 3. Process replies
        replies_base_path = os.path.join(thread_path, "replies")
        for reply_info in parsed_data.get("replies", []):
            reply_id = reply_info.get("reply_id")
            if not reply_id:
                logger.warning(f"Skipping reply due to missing ID: {str(reply_info)[:100]}")
                continue

            # Path for the current reply: .../{thread_id}/replies/{reply_id}/
            current_reply_path = os.path.join(replies_base_path, reply_id)
            _ensure_dir_exists(current_reply_path)

            # 3a. Save reply text content
            reply_text_content = reply_info.get("reply_text_content")
            if reply_text_content:
                reply_text_path = os.path.join(current_reply_path, "reply_text.json")
                try:
                    _save_json_content(reply_text_content, reply_text_path)
                    saved_files.append(reply_text_path)
                except VideoDownloadError as e:
                    logger.error(f"Failed to save reply text: {e}")

            # 3b. Download reply videos
            reply_videos_path = os.path.join(current_reply_path, "videos")
            for video_info in reply_info.get("reply_videos", []):
                video_url = video_info.get("video_url")
                # video_info.tweet_id here is actually the reply_id
                video_id_for_filename = video_info.get("tweet_id", reply_id)
                if video_url and video_id_for_filename:
                    try:
                        downloaded_path = download_video_content(video_url, video_id_for_filename, reply_videos_path, list_formats=list_formats)
                        saved_files.append(downloaded_path)
                    except VideoDownloadError as e:
                        logger.error(f"Failed to download reply video: {e}")
                        # Continue processing other videos

        logger.info(f"Finished processing and saving data for thread {user_screen_name}/{thread_id}. Total files saved/downloaded: {len(saved_files)}")
        return saved_files
        
    except (ValidationError, VideoDownloadError):
        # Re-raise our custom exceptions
        raise
    except Exception as e:
        # Wrap unexpected exceptions
        raise VideoDownloadError(f"Unexpected error saving parsed thread data: {str(e)}") from e
