
import time
import json
import os
from typing import Optional, Union, List
import re
from datetime import timedelta, datetime, timezone


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from dateutil.parser import isoparse

from ExtendedYoutubeEasyWrapper import ExtendedYoutubeEasyWrapper

def ISO8601_duration_to_time_delta(value: str) -> Optional[timedelta]:
    """
    function to convert ISO8601 relative periods (used for video durations) into a Python
    timedelta

    :param value: a string containing a ISO duration
    :return: duration as a timedelta
    """

    if value[0:2] == 'PT':
        analysis = re.findall(r'(\d+\D)', value[2:])

        if analysis is None:
            print(f'failed to process: {value}')
            return None
        else:
            min = 0
            sec = 0
            hour = 0
            for entry in analysis:
                if entry[-1] == 'M':
                    min = int(entry[:-1])
                elif entry[-1] == 'S':
                    sec = int(entry[:-1])
                elif entry[-1] == 'H':
                    hour = int(entry[:-1])
                else:
                    print('unhanded subsection {entry} in {value}')

            return timedelta(minutes=min, seconds=sec, hours=hour)
    else:
        print(f'string should start PT, {value}')
        return None

class SmartEveryDayDataSet:

    # YouTube Channel ID
    channel_ID = 'UC6107grRI4m0o2-emgoDnAA'
    # First ever Video
    channel_dawn = datetime(year=2010,
                                month=9,
                                day=1,
                                tzinfo=timezone.utc)

    # YouTube heavily restrict their API usage, to help manage daily allowances, this library uses
    # a data cache, stored in some JSON files
    _video_fn = 'smarter_everyday_videos.json'
    _detailed_video_fn = 'detailed_smarter_everyday_videos.json'

    def __init__(self, api_key):

        self.easy_wrapper = ExtendedYoutubeEasyWrapper()
        self.easy_wrapper.initialize(api_key=api_key)

        # populate the data and detailed meta data for all the videos
        self.videos = self.retrieve_videos()
        self.videos_detail = self.retrieve_videos_details()

    def retrieve_videos(self):

        return self.channel_videos(cache_file=self._video_fn,
                                   channel_id_list=[self.channel_ID],
                                   earliest_date=self.channel_dawn)

    def retrieve_videos_details(self):

        return self.video_details(cache_file=self._detailed_video_fn,
                                  videos=self.videos)


    def channel_videos(self, cache_file: str, earliest_date: datetime, channel_id_list: List[str]):
        """
        Search for Video published on a channel (or list of channels), this has two modes of
        operation, depending on whether the cache exists or not:
        - If the cache does not exist it retrieves all videos from earliest date
        - If the cache exists it looks for video after the last date in the cache

        This search returns some basic information in each video found

        :param cache_file: filename of the file to use as the cache
        :type cache_file: str
        :param earliest_date: date to start the search from
        :type earliest_date: datetime
        :param channel_id_list: list of the YouTube channel IDs to search
        :type channel_id_list: List[str]

        :return:
        """
        def get_videos(search_start_date):

            vid_list = []
            for channel_id in channel_id_list:
                vid_list += self.easy_wrapper.channel_videos(channelID=channel_id,
                                                             order='date',
                                                             publishedAfter=search_start_date)

            return vid_list

        if os.path.isfile(cache_file) is False:
            # The cache file does not exist and must be generated from scratch
            videos = get_videos(earliest_date)

            with open(cache_file, 'w') as fp:
                json.dump(videos, fp)

        else:
            # cache file exists and must be updated
            with open(cache_file) as fp:
                videos = json.load(fp)

            last_update_time = os.path.getmtime(cache_file)
            current_time = time.time()
            one_day_secs = 24 * 60 * 60
            if (current_time - one_day_secs) > last_update_time:
                # if the file was last updated more than 24 hours ago do an update

                # find the latest video publish date in the set of videos read from the cache
                video_ID = []
                last_date_of_interest = earliest_date
                for video in videos:
                    video_ID.append(video['video_id'])
                    video_pub_at = isoparse(video['publishedAt'])
                    if video_pub_at > last_date_of_interest:
                        last_date_of_interest = video_pub_at

                new_videos = get_videos(last_date_of_interest)

                # deduplicate any video found that may have been in the original list
                for video in new_videos:
                    if video['video_id'] in video_ID:
                        new_videos.remove(video)

                # if there are new videos found from the search, then write back out the cache file
                if len(new_videos) > 0:
                    videos += new_videos

                    with open(cache_file, 'w') as fp:
                        json.dump(videos, fp)
            else:
                print(f'{cache_file=} is less than 24 hours old no update performed')

        return videos

    @staticmethod
    def _get_video_id_list(videos):

        id_list = []
        for video in videos:
            id_list.append(video['video_id'])

        return id_list

    def video_details(self, cache_file, videos):
        """
        retrieve the details for a set of videos
        :param cache_file: filename for cache
        :param videos: video object list
        :return:
        """

        if os.path.isfile(cache_file) is False:
            videos_details = []
            for video in videos:
                video_detail = self.easy_wrapper.get_metadata(video_id=video['video_id'],
                                                              include_comments=False)
                videos_details.append(video_detail)


            with open(cache_file, 'w') as fp:
                json.dump(videos_details, fp)
        else:
            with open(cache_file) as fp:
                videos_details = json.load(fp)

            last_update_time = os.path.getmtime(cache_file)
            current_time = time.time()
            one_day_secs = 24 * 60 * 60
            if (current_time - one_day_secs) > last_update_time:
                # if the file was last updated more than 24 hours ago do an update

                video_id_list = self._get_video_id_list(videos=videos)
                detailed_video_id_list = self._get_video_id_list(videos=videos_details)

                for video_id in video_id_list:
                    if video_id not in detailed_video_id_list:

                        video_detail = self.easy_wrapper.get_metadata(video_id=video_id,
                                                                      include_comments=False)
                        videos_details.append(video_detail)

                    with open(cache_file, 'w') as fp:
                        json.dump(videos_details, fp)
            else:
                print(f'{cache_file=} is less than 24 hours old no update performed')


        return videos_details

    @property
    def DataFrame(self):
        """
        :return: panda Dataframe o of the videos and their details
        """

        # create a panda dataframe for the data
        duration_list = []
        published_list = []
        title_list = []
        stream_list = []
        views_list = []
        like_list = []
        dislike_list = []
        video_id_list = []
        channel_list = []

        videos = self.videos
        videos_detail = self.videos_detail

        for (video_summary, video_data) in zip(videos, videos_detail):

            duration = ISO8601_duration_to_time_delta(video_data['contentDetails']['duration'])
            if duration is None:
                continue

            assert video_summary['video_id'] == video_data['video_id']

            channel_list.append(video_summary['channel'])
            duration_list.append(duration.total_seconds())
            published_list.append(isoparse(video_data['publishedAt']))
            stream_list.append('liveStreamingDetails' in video_data.keys())
            views_list.append(int(video_data['statistics']['viewCount']))
            like_list.append(int(video_data['statistics']['likeCount']))
            dislike_list.append(int(video_data['statistics']['dislikeCount']))
            video_id_list.append(video_summary['video_id'])
            title_list.append(video_data['title'])

        DataFrame = pd.DataFrame({'Title': title_list,
                                        'Channel': channel_list,
                                        'Published Time': published_list,
                                        'Duration (s)': duration_list,
                                        'Stream': stream_list,
                                        'Likes': like_list,
                                        'Dislikes': dislike_list,
                                        'Views': views_list}, index=video_id_list)
        DataFrame['Like:Dislike Ratio'] = DataFrame['Likes'] / DataFrame['Dislikes']
        DataFrame['Like:Views Ratio'] = DataFrame['Likes'] / DataFrame['Views']
        DataFrame['Dislikes:Views Ratio'] = DataFrame['Dislikes'] / DataFrame['Views']
        DataFrame['Views Seconds'] = DataFrame['Duration (s)'] * DataFrame['Views']

        return DataFrame

if __name__ == "__main__":

    with open('.google_API_key') as fp:
        api_key=fp.readlines()

    data_class = SmartEveryDayDataSet(api_key=api_key)

    video_DataFrame = data_class.DataFrame

    # make a plot of the data
    fig, ax = plt.subplots(nrows=1, ncols=1, sharex=True, figsize=(16, 9))
    sns.scatterplot(data=video_DataFrame, x='Published Time', y='Duration (s)', size='Views', ax=ax)
    ax.set_title('Smart Every Day Videos')

    fig.savefig('SmarterEveryDay.png')
    #plt.close(fig)

    # save data to CSV file
    video_DataFrame.to_csv('SmartEveryDayVideos.csv')
