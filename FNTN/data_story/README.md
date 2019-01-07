# data/data_story

## Files
- story_table_twitter15_2018-05-12 19/37/23.391842.csv
- story_table_twitter16_2018-05-12 19/37/23.644850.csv
- FormattedStory_twitter1516.pkl

## Preprocess

### explicit-error-preprocessed
- Delete stories whose
    - writers are blocked.
    - link does not exist.
    - link does not have any contents.
- Done by hands.

### implicit-error-preprocessed
- Delete stories whose
    - link is other social media. (e.g. twitter, facebook)
    - main content is not text. (e.g. video, image, timeline)
    - length is less than 400.
- Done by hands.
 
### general-preprocessed
- Delete stories whose
    - link is expired.
- Delete multiple '\n' of contents.
- Delete sentences which are exact stopwords.

## Load
```python
# dump
get_formatted_stories(force_save=True).dump()

# load dumped file
formatted_stories: FormattedStory = get_formatted_stories()
```