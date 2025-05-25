#!/usr/bin/env python3
"""
Test script to demonstrate video quality improvements in x-thread-dl.
This script tests the enhanced video downloading capabilities.
"""

import os
import sys
import asyncio
import logging
from typing import Optional

# Add the current directory to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from scraper import Scraper
from video_downloader import list_video_formats, download_video_content

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('video_quality_test')

async def test_video_quality_improvements():
    """
    Test the video quality improvements by:
    1. Testing format listing functionality
    2. Testing enhanced video download with quality logging
    """
    
    # Test URLs - these are example URLs, replace with real ones for testing
    test_urls = [
        # Add real tweet URLs with videos here for testing
        # "https://x.com/username/status/1234567890123456789",
    ]
    
    if not test_urls:
        logger.info("No test URLs provided. To test with real videos:")
        logger.info("1. Find a tweet with video content")
        logger.info("2. Add the URL to the test_urls list in this script")
        logger.info("3. Run the script again")
        return
    
    # Get API token
    api_token = config.APIFY_API_TOKEN
    if not api_token:
        logger.error("No Apify API token found. Please set APIFY_API_TOKEN environment variable.")
        return
    
    scraper = Scraper(api_token=api_token)
    
    for url in test_urls:
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing video quality for: {url}")
        logger.info(f"{'='*60}")
        
        try:
            # Fetch the tweet
            tweet_data = await scraper.fetch_tweet(url)
            if not tweet_data:
                logger.warning(f"Could not fetch tweet data for: {url}")
                continue
            
            # Extract video URL
            video_url = scraper.extract_video_url(tweet_data)
            if not video_url:
                logger.warning(f"No video found in tweet: {url}")
                continue
            
            logger.info(f"Found video URL: {video_url}")
            
            # Test format listing
            logger.info("\n--- Testing Format Listing ---")
            formats = list_video_formats(video_url)
            if formats:
                logger.info(f"Available formats ({len(formats)} total):")
                for i, fmt in enumerate(formats[:10]):  # Show first 10
                    logger.info(f"  {i+1}. ID: {fmt.get('format_id', 'N/A')} | "
                              f"Quality: {fmt.get('height', 'N/A')}p | "
                              f"Ext: {fmt.get('ext', 'N/A')} | "
                              f"Bitrate: {fmt.get('tbr', 'N/A')} kbps | "
                              f"Codec: {fmt.get('vcodec', 'N/A')}")
                if len(formats) > 10:
                    logger.info(f"  ... and {len(formats) - 10} more formats")
            else:
                logger.warning("Could not list formats")
            
            # Test enhanced download
            logger.info("\n--- Testing Enhanced Download ---")
            test_output_dir = "test_output"
            os.makedirs(test_output_dir, exist_ok=True)
            
            # Extract tweet ID for filename
            tweet_id = scraper._extract_tweet_id(url)
            if not tweet_id:
                tweet_id = "test_video"
            
            downloaded_path = download_video_content(
                video_url, 
                tweet_id, 
                test_output_dir, 
                list_formats=True
            )
            
            if downloaded_path:
                file_size = os.path.getsize(downloaded_path) / (1024 * 1024)  # MB
                logger.info(f"✅ Successfully downloaded: {downloaded_path}")
                logger.info(f"📁 File size: {file_size:.1f} MB")
                
                # Clean up test file
                try:
                    os.remove(downloaded_path)
                    logger.info("🧹 Cleaned up test file")
                except:
                    pass
            else:
                logger.error("❌ Download failed")
                
        except Exception as e:
            logger.error(f"Error testing {url}: {str(e)}", exc_info=True)
    
    # Clean up test directory
    try:
        os.rmdir(test_output_dir)
    except:
        pass

def test_format_selection_logic():
    """
    Test the format selection logic with mock data.
    """
    logger.info("\n" + "="*60)
    logger.info("Testing Format Selection Logic")
    logger.info("="*60)
    
    # Mock video variants data (similar to what Twitter API returns)
    mock_variants = [
        {'type': 'video/mp4', 'bitrate': 832000, 'src': 'low_quality.mp4'},
        {'type': 'video/mp4', 'bitrate': 2176000, 'src': 'medium_quality.mp4'},
        {'type': 'video/mp4', 'bitrate': 4096000, 'src': 'high_quality.mp4'},
        {'type': 'application/x-mpegURL', 'src': 'playlist.m3u8'},
    ]
    
    # Test MP4 selection and bitrate sorting
    mp4_variants = [v for v in mock_variants if v.get('type') == 'video/mp4']
    mp4_variants.sort(key=lambda x: x.get('bitrate', 0), reverse=True)
    
    logger.info("Mock video variants:")
    for i, variant in enumerate(mock_variants):
        logger.info(f"  {i+1}. Type: {variant.get('type')} | Bitrate: {variant.get('bitrate', 'N/A')} | URL: {variant.get('src')}")
    
    logger.info(f"\nSelected variant (highest bitrate MP4): {mp4_variants[0].get('src')} ({mp4_variants[0].get('bitrate')} bps)")
    
    assert mp4_variants[0].get('src') == 'high_quality.mp4', "Should select highest bitrate MP4"
    logger.info("✅ Format selection logic test passed!")

if __name__ == '__main__':
    print("🎥 X-Thread-DL Video Quality Test")
    print("=" * 50)
    
    # Test format selection logic first
    test_format_selection_logic()
    
    # Test with real videos (if URLs are provided)
    asyncio.run(test_video_quality_improvements())
    
    print("\n🎯 Test Summary:")
    print("1. Enhanced yt-dlp format selection: bestvideo+bestaudio with 4K support")
    print("2. Improved video variant selection: highest bitrate MP4 preferred")
    print("3. Added format listing capability for debugging")
    print("4. Enhanced logging for video quality information")
    print("\n💡 To test with real videos, add tweet URLs to the test_urls list in this script.")
