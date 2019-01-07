# data/data_event

## Files
- event_table_twitter15_2018-05-09 15/25/11.355233.csv
- event_table_twitter16_2018-05-09 15/25/20.939891.csv
- FormattedEvent_twitter1516.pkl

## Preprocess
- Delete events whose stories are removed from preprocessing.

## Load
```python
formatted_stories = get_formatted_stories()

# dump
get_formatted_events(formatted_stories.tweet_id_to_story_id, force_save=True).dump()

# load dumped file
formatted_events: FormattedEvent = get_formatted_events(tweet_id_to_story_id=formatted_stories.tweet_id_to_story_id)
```