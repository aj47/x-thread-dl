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
from exceptions import ScrapingError, ValidationError

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
            
        Raises:
            ValidationError: If no valid API token is provided.
        """
        self.api_token = api_token or config.APIFY_API_TOKEN
        if not self.api_token:
            raise ValidationError("No Apify API token provided. Scraping cannot proceed without a valid token.")

        # Initialize the Apify client
        try:
            self.client = ApifyClient(token=self.api_token)
        except Exception as e:
            raise ScrapingError(f"Failed to initialize Apify client: {str(e)}") from e

    async def fetch_tweet(self, url: str) -> Dict[str, Any]:
        """
        Fetch a tweet using Apify's Twitter Scraper.

        Args:
            url (str): The X.com (Twitter) URL to scrape

        Returns:
            Dict[str, Any]: The tweet data
            
        Raises:
            ValidationError: If URL is invalid or tweet ID cannot be extracted.
            ScrapingError: If scraping fails or no data is returned.
        """
        try:
            logger.info(f"Fetching tweet from URL: {url}")

            # Extract tweet ID from URL
            tweet_id = self._extract_tweet_id(url)
            if not tweet_id:
                raise ValidationError(f"Could not extract tweet ID from URL: {url}")

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
            try:
                run = await loop.run_in_executor(
                    None,
                    lambda: self.client.actor(config.TWITTER_SCRAPER_ACTOR_ID).call(run_input=input_data)
                )
            except Exception as e:
                raise ScrapingError(f"Failed to execute Apify actor for URL {url}: {str(e)}") from e

            # Get the dataset items
            try:
                dataset_items = await loop.run_in_executor(
                    None,
                    lambda: self.client.dataset(run["defaultDatasetId"]).list_items().items
                )
            except Exception as e:
                raise ScrapingError(f"Failed to retrieve dataset items for URL {url}: {str(e)}") from e

            if not dataset_items:
                raise ScrapingError(f"No tweet data found for URL: {url}")

            # Get the first (and should be only) item
            tweet_data = dataset_items[0]

            # Log success
            logger.info(f"Successfully fetched tweet from URL: {url}")

            return tweet_data

        except (ValidationError, ScrapingError):
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            # Wrap unexpected exceptions
            raise ScrapingError(f"Unexpected error fetching tweet from URL {url}: {str(e)}") from e

    async def fetch_tweet_replies(self, url: str, limit: int = config.DEFAULT_REPLY_LIMIT) -> List[Dict[str, Any]]:
        """
        Fetch replies to a tweet using Apify's Twitter Replies Scraper.

        Args:
            url (str): The X.com (Twitter) URL to scrape replies from
            limit (int): Maximum number of replies to fetch

        Returns:
            List[Dict[str, Any]]: List of reply data
            
        Raises:
            ValidationError: If URL is invalid or limit is invalid.
            ScrapingError: If scraping fails.
        """
        try:
            if limit <= 0:
                raise ValidationError(f"Reply limit must be positive, got: {limit}")
                
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
            try:
                run = await loop.run_in_executor(
                    None,
                    lambda: self.client.actor(config.TWITTER_REPLIES_SCRAPER_ACTOR_ID).call(run_input=input_data)
                )
            except Exception as e:
                raise ScrapingError(f"Failed to execute replies scraper for URL {url}: {str(e)}") from e

            # Get the dataset items
            try:
                dataset_items = await loop.run_in_executor(
                    None,
                    lambda: self.client.dataset(run["defaultDatasetId"]).list_items().items
                )
            except Exception as e:
                raise ScrapingError(f"Failed to retrieve reply dataset items for URL {url}: {str(e)}") from e

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

        except (ValidationError, ScrapingError):
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            # Wrap unexpected exceptions
            raise ScrapingError(f"Unexpected error fetching tweet replies from URL {url}: {str(e)}") from e

    async def fetch_tweet_and_replies(self, url: str, reply_limit: int = config.DEFAULT_REPLY_LIMIT) -> Dict[str, Any]:
        """
        Fetch a tweet and its replies.

        Args:
            url (str): The X.com (Twitter) URL to scrape
            reply_limit (int): Maximum number of replies to fetch

        Returns:
            Dict[str, Any]: Dictionary containing the tweet and its replies
            
        Raises:
            ValidationError: If URL or reply_limit is invalid.
            ScrapingError: If scraping fails.
        """
        try:
            # Fetch the tweet
            tweet = await self.fetch_tweet(url)

            # Fetch the replies
            replies = await self.fetch_tweet_replies(url, reply_limit)

            return {
                "tweet": tweet,
                "replies": replies
            }
        except (ValidationError, ScrapingError):
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            # Wrap unexpected exceptions
            raise ScrapingError(f"Unexpected error fetching tweet and replies from URL {url}: {str(e)}") from e

    def extract_video_url(self, tweet_data: Dict[str, Any]) -> Optional[str]:
        """
        Extract the highest quality video URL from tweet data if it exists.

        Args:
            tweet_data (Dict[str, Any]): The tweet data

        Returns:
            Optional[str]: The highest quality video URL or None if no video exists
            
        Raises:
            ScrapingError: If tweet_data is invalid or extraction fails unexpectedly.
        """
        try:
            if not tweet_data or not isinstance(tweet_data, dict):
                raise ScrapingError("Invalid tweet data provided for video extraction")
                
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
        except ScrapingError:
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            raise ScrapingError(f"Unexpected error extracting video URL from tweet data: {str(e)}") from e

    def _extract_tweet_id(self, url: str) -> Optional[str]:
        """
        Extract the tweet ID from an X.com (Twitter) URL.

        Args:
            url (str): The X.com (Twitter) URL

        Returns:
            Optional[str]: The tweet ID or None if extraction failed
            
        Raises:
            ValidationError: If URL format is invalid.
        """
        try:
            if not url or not isinstance(url, str):
                raise ValidationError("URL must be a non-empty string")
                
            # Pattern to match tweet IDs in X.com (Twitter) URLs
            pattern = r'(?:twitter\.com|x\.com)/\w+/status/(\d+)'
            match = re.search(pattern, url)

            if match:
                return match.group(1)

            # Log the URL and pattern when no match is found
            logger.debug(f"No tweet ID found in URL: {url} using pattern: {pattern}")

            return None
        except ValidationError:
            # Re-raise validation errors
            raise
        except Exception as e:
            raise ValidationError(f"Error extracting tweet ID from URL {url}: {str(e)}") from e

    def _format_url(self, url: str) -> str:
        """
        Ensure URL is properly formatted.

        Args:
            url (str): The URL to format

        Returns:
            str: The formatted URL
            
        Raises:
            ValidationError: If URL is invalid.
        """
        try:
            if not url or not isinstance(url, str):
                raise ValidationError("URL must be a non-empty string")
                
            if not url.startswith('http'):
                return f"https://{url}"
            return url
        except Exception as e:
            raise ValidationError(f"Error formatting URL {url}: {str(e)}") from e
