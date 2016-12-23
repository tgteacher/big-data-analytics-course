#!/usr/bin/env python

import argparse
import random

def pretty_print(i,followed_users):
    line=str(i)+"\t"
    first=True
    for x in followed_users:
        if first:
            line+=" "
        line+=str(x)
    print line

if __name__ == "__main__":
    parser= argparse.ArgumentParser(description="Generates users of a directed social network.")
    parser.add_argument("n_users",type=int,help="Number of users")
    parser.add_argument("max_followed",type=int,help="Max number of followed users")
    args = parser.parse_args()
    n_users = args.n_users
    max_followed = args.max_followed
    for i in range(1,n_users+1):
        # Randomly pick the number of users that i follows
        n_followed=random.randint(1,max_followed)
        followed_users=[]
        for j in range(1,n_followed+1):
            followed_user=i
            while followed_user == i or followed_user in followed_users:
                followed_user=random.randint(1,n_users)
            followed_users.append(followed_user)
        pretty_print(i,followed_users)
