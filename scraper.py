"""
Scraper module for the x-thread-dl tool.
Handles fetching tweets and replies using the Apify API.
"""

import asyncio
import re
import logging
from typing import Optional, Dict, List, Any, Union
from apify_client import ApifyClient

# Import configuration
import config

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('x-thread-dl.scraper')

class Scraper:
    """Class for scraping tweets and replies from X.com (Twitter)."""

    def __init__(self, api_token: Optional[str] = None):
        """
        Initialize the scraper with an Apify API token.

        Args:
            api_token (Optional[str]): The Apify API token. If None, uses the token from config.
        """
        self.api_token = api_token or config.APIFY_API_TOKEN
        if not self.api_token:
            logger.warning("No Apify API token provided. Scraping will likely fail.")

        # Initialize the Apify client
        self.client = ApifyClient(token=self.api_token)

    async def fetch_tweet(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a tweet using Apify's Twitter Scraper.

        Args:
            url (str): The X.com (Twitter) URL to scrape

        Returns:
            Optional[Dict[str, Any]]: The tweet data or None if scraping failed
        """
        try:
            logger.info(f"Fetching tweet from URL: {url}")

            # Extract tweet ID from URL
            tweet_id = self._extract_tweet_id(url)
            if not tweet_id:
                logger.error(f"Could not extract tweet ID from URL: {url}")
                return None

            # Ensure URL is properly formatted
            formatted_url = self._format_url(url)
            logger.info(f"Using formatted URL: {formatted_url}")

            # Prepare the input for the Twitter Scraper actor
            input_data = {
                "startUrls": [{"url": formatted_url}],
                "tweetsDesired": 1,
                "addUserInfo": True,
                "proxyConfig": {
                    "useApifyProxy": True
                }
            }

            # Use a separate thread for the blocking API call
            loop = asyncio.get_event_loop()
            run = await loop.run_in_executor(
                None,
                lambda: self.client.actor(config.TWITTER_SCRAPER_ACTOR_ID).call(run_input=input_data)
            )

            # Get the dataset items
            dataset_items = await loop.run_in_executor(
                None,
                lambda: self.client.dataset(run["defaultDatasetId"]).list_items().items
            )

            if not dataset_items:
                logger.warning(f"No tweet data found for URL: {url}")
                return None

            # Get the first (and should be only) item
            tweet_data = dataset_items[0]

            # Log success
            logger.info(f"Successfully fetched tweet from URL: {url}")

            return tweet_data

        except Exception as e:
            logger.error(f"Error fetching tweet from URL {url}: {str(e)}", exc_info=True)
            return None

    async def fetch_tweet_replies(self, url: str, limit: int = config.DEFAULT_REPLY_LIMIT) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch replies to a tweet using Apify's Twitter Replies Scraper.

        Args:
            url (str): The X.com (Twitter) URL to scrape replies from
            limit (int): Maximum number of replies to fetch

        Returns:
            Optional[List[Dict[str, Any]]]: List of reply data or None if scraping failed
        """
        try:
            logger.info(f"Fetching up to {limit} tweet replies from URL: {url}")

            # Ensure URL is properly formatted
            formatted_url = self._format_url(url)
            logger.info(f"Using formatted URL for replies: {formatted_url}")

            # Prepare the input for the Twitter Replies Scraper actor
            input_data = {
                "postUrls": [formatted_url],
                "resultsLimit": limit
            }

            # Use a separate thread for the blocking API call
            loop = asyncio.get_event_loop()
            run = await loop.run_in_executor(
                None,
                lambda: self.client.actor(config.TWITTER_REPLIES_SCRAPER_ACTOR_ID).call(run_input=input_data)
            )

            # Get the dataset items
            dataset_items = await loop.run_in_executor(
                None,
                lambda: self.client.dataset(run["defaultDatasetId"]).list_items().items
            )

            if not dataset_items:
                logger.warning(f"No reply data found for URL: {url}")
                return []

            # Log success
            logger.info(f"Successfully fetched {len(dataset_items)} replies from URL: {url}")

            # Debug: Log the keys of each reply object to diagnose tweet ID extraction issues
            for i, reply in enumerate(dataset_items):
                logger.debug(f"Reply {i} keys: {list(reply.keys())}")
                # Debug: Log the full structure of the first reply to help diagnose author extraction issues
                if i == 0:
                    logger.debug(f"Reply 0 full structure: {reply}")

            return dataset_items

        except Exception as e:
            logger.error(f"Error fetching tweet replies from URL {url}: {str(e)}", exc_info=True)
            return None

    async def fetch_tweet_and_replies(self, url: str, reply_limit: int = config.DEFAULT_REPLY_LIMIT) -> Dict[str, Any]:
        """
        Fetch a tweet and its replies.

        Args:
            url (str): The X.com (Twitter) URL to scrape
            reply_limit (int): Maximum number of replies to fetch

        Returns:
            Dict[str, Any]: Dictionary containing the tweet and its replies
        """
        # Fetch the tweet
        tweet = await self.fetch_tweet(url)

        # Fetch the replies
        replies = await self.fetch_tweet_replies(url, reply_limit)

        return {
            "tweet": tweet,
            "replies": replies or []
        }

    def extract_video_url(self, tweet_data: Dict[str, Any]) -> Optional[str]:
        """
        Extract the highest quality video URL from tweet data if it exists.

        Args:
            tweet_data (Dict[str, Any]): The tweet data

        Returns:
            Optional[str]: The highest quality video URL or None if no video exists
        """
        try:
            # Check if video exists in the tweet data
            if 'video' in tweet_data and tweet_data['video'] and 'variants' in tweet_data['video']:
                variants = tweet_data['video']['variants']

                # Log available variants for debugging
                logger.debug(f"Found {len(variants)} video variants")
                for i, variant in enumerate(variants):
                    logger.debug(f"Variant {i}: type={variant.get('type')}, bitrate={variant.get('bitrate')}")

                # Prefer MP4 format and sort by bitrate (highest first)
                mp4_variants = [v for v in variants if v.get('type') == 'video/mp4']

                if mp4_variants:
                    # Sort by bitrate if available, highest first for best quality
                    mp4_variants.sort(key=lambda x: x.get('bitrate', 0), reverse=True)
                    best_variant = mp4_variants[0]
                    logger.info(f"Selected MP4 variant with bitrate: {best_variant.get('bitrate', 'unknown')}")
                    return best_variant.get('src')

                # If no MP4 variants, sort all variants by bitrate and take the best
                if variants:
                    variants.sort(key=lambda x: x.get('bitrate', 0), reverse=True)
                    best_variant = variants[0]
                    logger.info(f"Selected variant (non-MP4) with bitrate: {best_variant.get('bitrate', 'unknown')}")
                    return best_variant.get('src')

            # Check mediaDetails as an alternative
            if 'mediaDetails' in tweet_data:
                for media in tweet_data['mediaDetails']:
                    if media.get('type') == 'video' and 'video_info' in media and 'variants' in media['video_info']:
                        variants = media['video_info']['variants']

                        # Log available variants for debugging
                        logger.debug(f"Found {len(variants)} video variants in mediaDetails")
                        for i, variant in enumerate(variants):
                            logger.debug(f"MediaDetails variant {i}: content_type={variant.get('content_type')}, bitrate={variant.get('bitrate')}")

                        # Prefer MP4 format and sort by bitrate (highest first)
                        mp4_variants = [v for v in variants if v.get('content_type') == 'video/mp4']

                        if mp4_variants:
                            # Sort by bitrate if available, highest first for best quality
                            mp4_variants.sort(key=lambda x: x.get('bitrate', 0), reverse=True)
                            best_variant = mp4_variants[0]
                            logger.info(f"Selected MP4 variant from mediaDetails with bitrate: {best_variant.get('bitrate', 'unknown')}")
                            return best_variant.get('url')

                        # If no MP4 variants, sort all variants by bitrate and take the best
                        if variants:
                            variants.sort(key=lambda x: x.get('bitrate', 0), reverse=True)
                            best_variant = variants[0]
                            logger.info(f"Selected variant (non-MP4) from mediaDetails with bitrate: {best_variant.get('bitrate', 'unknown')}")
                            return best_variant.get('url')

            logger.debug("No video found in tweet data")
            return None
        except Exception as e:
            logger.error(f"Error extracting video URL from tweet data: {str(e)}", exc_info=True)
            return None

    def _extract_tweet_id(self, url: str) -> Optional[str]:
        """
        Extract the tweet ID from an X.com (Twitter) URL.

        Args:
            url (str): The X.com (Twitter) URL

        Returns:
            Optional[str]: The tweet ID or None if extraction failed
        """
        try:
            # Pattern to match tweet IDs in X.com (Twitter) URLs
            pattern = r'(?:twitter\.com|x\.com)/\w+/status/(\d+)'
            match = re.search(pattern, url)

            if match:
                return match.group(1)

            # Log the URL and pattern when no match is found
            logger.debug(f"No tweet ID found in URL: {url} using pattern: {pattern}")

            return None
        except Exception as e:
            logger.error(f"Error extracting tweet ID from URL {url}: {str(e)}", exc_info=True)
            return None

    def _format_url(self, url: str) -> str:
        """
        Ensure URL is properly formatted.

        Args:
            url (str): The URL to format

        Returns:
            str: The formatted URL
        """
        if not url.startswith('http'):
            return f"https://{url}"
        return url
