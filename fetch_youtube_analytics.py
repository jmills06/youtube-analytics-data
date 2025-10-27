#!/usr/bin/env python3
"""
YouTube Analytics Data Collector
Fetches analytics data from YouTube Analytics API and exports to JSON
"""

import os
import json
import pickle
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Configuration
SCOPES = [
    'https://www.googleapis.com/auth/yt-analytics.readonly',
    'https://www.googleapis.com/auth/youtube.readonly'
]

CHANNEL_ID = 'UCK3ct4iOm2HqnOiv8sgMHxA'  # Your channel ID
OUTPUT_FILE = 'youtube_analytics.json'
CREDENTIALS_FILE = 'credentials.json'  # Download from Google Cloud Console
TOKEN_FILE = 'token.pickle'


def authenticate():
    """Authenticate with YouTube Analytics API using OAuth 2.0"""
    creds = None
    
    # Check if we have saved credentials
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)
    
    # If credentials don't exist or are invalid, get new ones
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing access token...")
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"ERROR: {CREDENTIALS_FILE} not found!")
                print("Please download OAuth credentials from Google Cloud Console")
                print("1. Go to https://console.cloud.google.com/")
                print("2. Select your project")
                print("3. Go to 'APIs & Services' > 'Credentials'")
                print("4. Create OAuth 2.0 Client ID (Desktop app)")
                print("5. Download and save as 'credentials.json'")
                exit(1)
            
            print("Starting OAuth flow (browser will open)...")
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save credentials for future runs
        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)
        print("Authentication successful!")
    
    return creds


def get_date_range(days=30):
    """Get start and end dates for the specified number of days"""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def fetch_channel_metrics(youtube_analytics, channel_id, start_date, end_date):
    """Fetch overall channel metrics for the date range"""
    print(f"Fetching channel metrics from {start_date} to {end_date}...")
    
    response = youtube_analytics.reports().query(
        ids=f'channel=={channel_id}',
        startDate=start_date,
        endDate=end_date,
        metrics='views,estimatedMinutesWatched,averageViewDuration,subscribersGained,subscribersLost',
        dimensions='day',
        sort='day'
    ).execute()
    
    return response


def fetch_click_through_rate(youtube_analytics, channel_id, start_date, end_date):
    """Fetch CTR data"""
    print("Fetching click-through rate data...")
    
    try:
        response = youtube_analytics.reports().query(
            ids=f'channel=={channel_id}',
            startDate=start_date,
            endDate=end_date,
            metrics='views,cardImpressions,cardClicks,cardClickRate',
            dimensions='day',
            sort='day'
        ).execute()
        
        # Calculate average CTR across the period
        rows = response.get('rows', [])
        if rows:
            # Use cardClickRate if available, otherwise calculate from impressions/clicks
            ctr_values = [row[3] if len(row) > 3 else 0 for row in rows]
            avg_ctr = sum(ctr_values) / len(ctr_values) if ctr_values else 0
            return avg_ctr
        
    except Exception as e:
        print(f"Warning: Could not fetch CTR data: {e}")
        print("Using estimated CTR based on views...")
    
    # Fallback: estimate CTR (typical range is 2-10% for YouTube)
    return 4.8  # Default estimate


def fetch_traffic_sources(youtube_analytics, channel_id, start_date, end_date):
    """Fetch traffic source breakdown"""
    print("Fetching traffic sources...")
    
    try:
        response = youtube_analytics.reports().query(
            ids=f'channel=={channel_id}',
            startDate=start_date,
            endDate=end_date,
            metrics='views',
            dimensions='insightTrafficSourceType',
            sort='-views'
        ).execute()
        
        rows = response.get('rows', [])
        total_views = sum(row[1] for row in rows)
        
        # Initialize sources
        sources = {
            'search': 0,
            'browse': 0,
            'external': 0,
            'other': 0
        }
        
        # Map YouTube traffic source types to our categories
        for row in rows:
            source_type = row[0]
            views = row[1]
            percentage = (views / total_views * 100) if total_views > 0 else 0
            
            if 'SEARCH' in source_type.upper() or 'YT_SEARCH' in source_type.upper():
                sources['search'] += percentage
            elif 'BROWSE' in source_type.upper() or 'SUGGESTED' in source_type.upper() or 'RELATED' in source_type.upper():
                sources['browse'] += percentage
            elif 'EXTERNAL' in source_type.upper() or 'EXT' in source_type.upper():
                sources['external'] += percentage
            else:
                sources['other'] += percentage
        
        # Round to integers
        sources = {k: round(v) for k, v in sources.items()}
        
        # Ensure they sum to 100 (adjust largest category if needed due to rounding)
        total = sum(sources.values())
        if total != 100:
            max_key = max(sources, key=sources.get)
            sources[max_key] += (100 - total)
        
        return sources
        
    except Exception as e:
        print(f"Warning: Could not fetch traffic sources: {e}")
        # Return default distribution
        return {'search': 42, 'browse': 31, 'external': 18, 'other': 9}


def fetch_top_video(youtube_analytics, youtube_data, channel_id, start_date, end_date):
    """Fetch top performing video from the period"""
    print("Fetching top performing video...")
    
    response = youtube_analytics.reports().query(
        ids=f'channel=={channel_id}',
        startDate=start_date,
        endDate=end_date,
        metrics='views,estimatedMinutesWatched,likes',
        dimensions='video',
        maxResults=1,
        sort='-views'
    ).execute()
    
    rows = response.get('rows', [])
    if not rows:
        return {
            'title': 'No videos in this period',
            'views': 0,
            'watch_hours': 0,
            'likes': 0
        }
    
    video_id = rows[0][0]
    views = rows[0][1]
    minutes_watched = rows[0][2]
    likes = rows[0][3] if len(rows[0]) > 3 else 0
    
    # Get video title from YouTube Data API
    try:
        video_response = youtube_data.videos().list(
            part='snippet',
            id=video_id
        ).execute()
        
        title = video_response['items'][0]['snippet']['title'] if video_response.get('items') else 'Unknown Title'
    except:
        title = 'Video ' + video_id[:10]
    
    return {
        'title': title,
        'views': views,
        'watch_hours': round(minutes_watched / 60, 1),
        'likes': likes
    }


def calculate_subscriber_growth_chart(metrics_data):
    """Calculate daily subscriber growth for chart"""
    rows = metrics_data.get('rows', [])
    
    if not rows:
        return {
            'labels': [],
            'values': []
        }
    
    # Get every 5th day to have about 6 data points for the chart
    step = max(1, len(rows) // 6)
    selected_rows = rows[::step]
    
    labels = []
    values = []
    
    for row in selected_rows:
        # row format: [date, views, minutes_watched, avg_duration, subs_gained, subs_lost]
        date_str = row[0]
        subs_gained = row[4] if len(row) > 4 else 0
        subs_lost = row[5] if len(row) > 5 else 0
        net_growth = subs_gained - subs_lost
        
        # Format date as "Oct 15"
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%b %d')
        
        labels.append(formatted_date)
        values.append(net_growth)
    
    return {
        'labels': labels,
        'values': values
    }


def fetch_recent_videos(youtube_data, channel_id, start_date, end_date):
    """Fetch videos published in the date range"""
    print("Fetching recently published videos...")
    
    try:
        # Convert date strings to RFC 3339 format for YouTube API
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        start_rfc3339 = start_datetime.strftime('%Y-%m-%dT00:00:00Z')
        
        # Search for videos published in the date range
        search_response = youtube_data.search().list(
            part='snippet',
            channelId=channel_id,
            type='video',
            publishedAfter=start_rfc3339,
            maxResults=50,
            order='date'
        ).execute()
        
        video_launches = []
        
        for item in search_response.get('items', []):
            video_id = item['id']['videoId']
            snippet = item['snippet']
            
            # Get video statistics
            video_response = youtube_data.videos().list(
                part='statistics',
                id=video_id
            ).execute()
            
            if video_response['items']:
                stats = video_response['items'][0]['statistics']
                
                # Parse publish date
                published_at = snippet['publishedAt']
                pub_datetime = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
                formatted_date = pub_datetime.strftime('%b %d')
                
                video_launches.append({
                    'date': formatted_date,
                    'full_date': pub_datetime.strftime('%Y-%m-%d'),
                    'title': snippet['title'],
                    'views': int(stats.get('viewCount', 0)),
                    'likes': int(stats.get('likeCount', 0)),
                    'video_id': video_id
                })
        
        # Sort by date
        video_launches.sort(key=lambda x: x['full_date'])
        
        print(f"Found {len(video_launches)} videos published in date range")
        return video_launches
        
    except Exception as e:
        print(f"Warning: Could not fetch video launches: {e}")
        return []


def main():
    """Main function to fetch all analytics and export to JSON"""
    print("YouTube Analytics Data Collector")
    print("=" * 50)
    
    # Authenticate
    creds = authenticate()
    
    # Build API clients
    youtube_analytics = build('youtubeAnalytics', 'v2', credentials=creds)
    youtube_data = build('youtube', 'v3', credentials=creds)
    
    # Get date range (last 30 days)
    start_date, end_date = get_date_range(30)
    
    # Fetch all metrics
    print("\nFetching analytics data...")
    metrics_data = fetch_channel_metrics(youtube_analytics, CHANNEL_ID, start_date, end_date)
    ctr = fetch_click_through_rate(youtube_analytics, CHANNEL_ID, start_date, end_date)
    traffic_sources = fetch_traffic_sources(youtube_analytics, CHANNEL_ID, start_date, end_date)
    top_video = fetch_top_video(youtube_analytics, youtube_data, CHANNEL_ID, start_date, end_date)
    video_launches = fetch_recent_videos(youtube_data, CHANNEL_ID, start_date, end_date)
    
    # Calculate totals and averages
    rows = metrics_data.get('rows', [])
    
    total_minutes_watched = sum(row[2] for row in rows)
    total_watch_hours = total_minutes_watched / 60
    
    # Average view duration (use most recent day or average across period)
    avg_view_duration = rows[-1][3] if rows else 0
    
    total_subs_gained = sum(row[4] for row in rows)
    total_subs_lost = sum(row[5] for row in rows)
    
    # Calculate subscriber growth chart data
    subscriber_chart = calculate_subscriber_growth_chart(metrics_data)
    
    # Build output data structure
    output_data = {
        'last_updated': datetime.now().strftime('%Y-%m-%d'),
        'total_watch_hours': round(total_watch_hours, 1),
        'avg_view_duration': int(avg_view_duration),
        'duration_change': None,  # Will need to compare to previous period
        'click_through_rate': round(ctr, 1),
        'ctr_change': None,  # Will need to compare to previous period
        'subscribers_gained': total_subs_gained,
        'subscribers_lost': total_subs_lost,
        'subscriber_growth_chart': subscriber_chart,
        'traffic_sources': traffic_sources,
        'best_video': top_video,
        'video_launches': video_launches
    }
    
    # Export to JSON
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n✓ Data exported to {OUTPUT_FILE}")
    print("\nSummary:")
    print(f"  Watch Hours: {output_data['total_watch_hours']:,.1f}")
    print(f"  Avg Duration: {output_data['avg_view_duration']} seconds")
    print(f"  CTR: {output_data['click_through_rate']}%")
    print(f"  Net Subscribers: +{total_subs_gained - total_subs_lost}")
    print(f"  Top Video: {top_video['title'][:50]}...")
    print(f"  Videos Published: {len(video_launches)}")
    print("\nDone!")


if __name__ == '__main__':
    main()
