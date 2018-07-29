# data/data_alignment

## media_alignment(Original Bakshy et al.)
- Refer to [Bakshy, Eytan, Solomon Messing, and Lada A. Adamic. "Exposure to ideologically diverse news and opinion on Facebook." Science 348.6239 (2015): 1130-1132.](http://science.sciencemag.org/content/348/6239/1130)

## reviewed_media_alignment_in_twitter
- Add twitter accounts to 'media_alignment(Original Bakshy et al.)'
- For each row (domain), we chose at most one account by applying the following rules in order:
    1. an account that webpage of domain introduces.
    2. a verified account with the domain.
    3. an account that states itself as an 'official account of the domain'.
    4. an account that mainly posts the webpage of the domain.


## reviewed_media_alignment_with_twitter_id
- Add twitter id to reviewed_media_alignment_in_twitter
- Remove some rows which are:
    - not news media (twitter.com, en.wikipedia.org, www.youtube.com, m.youtube.com).
    - related to the whitehouse (www.whitehouse.gov, petitions.whitehouse.gov), since there was a change of government in 2016.