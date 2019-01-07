# data/data_network

## Files
- UserNetwork_test.pkl (small file: 7 users, 1 error users)
- UserNetwork.pkl (full file: 79416 users, 9922 error users)

## Description
- error users is a subset of users.
```python
user_network.user_id_to_friend_ids = ['123': [234, 345], '456': None]
user_network.error_user_set = {'456'}
```

## Load
```python
# Load 'UserNetwork.pkl'
user_network = UserNetwork()
user_network.load()

# Or load specific name of file (.pkl)
user_network = UserNetwork('file.pkl')
user_network.load()
```
