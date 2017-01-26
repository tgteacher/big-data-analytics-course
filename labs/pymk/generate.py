#!/usr/bin/env python

import argparse
import random

def pretty_print(friends):
    for user_id in friends.keys():
        line=str(user_id)+"\t"
        for x in sorted(friends[user_id]):
            line+=" "
            line+=str(x)
        print line

if __name__ == "__main__":
    parser= argparse.ArgumentParser(description="Generates users of a non-directed social network.")
    parser.add_argument("n_users",type=int,help="Number of users")
    parser.add_argument("max_friends",type=int,help="Max number of friends")
    args = parser.parse_args()
    n_users = args.n_users
    max_friends = args.max_friends
    friends = {}
    for user_id in range(1,n_users+1):
        # Randomly pick the number of friends of user i
        n_friends=random.randint(1,max_friends)
        if not (user_id in friends.keys()):  # create list if it doesn't exist
            friends[user_id] = []
        # User i may already have friends created from another user's list:
        # check how many more friends we need to create
        n_friends_to_create = n_friends - len(friends[user_id]) # this might be a negative number
        for i in range(1,n_friends_to_create+1):
            friend_id = user_id
            while friend_id == user_id or friend_id in friends[user_id]:
                friend_id=random.randint(1,n_users)
            # Add friend_id to user_id's friends
            friends[user_id].append(friend_id)
            # Add user_id to friend_id's friends
            if not (friend_id in friends.keys()):
                friends[friend_id] = []  # create list if it doesn't exist
            friends[friend_id].append(user_id)
    # Print all users
    pretty_print(friends)
