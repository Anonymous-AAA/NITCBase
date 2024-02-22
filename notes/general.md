# Trivia
1. Is OpenRelTable a necessary data structure?
A. Yes, it combines common functionalities required by both attribute cache and relation cache.

# Doubts
1. In BlockAccess::insert while adding a new block to the relation we dont update lblock of the new block ?
A. Yes, we are doing it.
2. Stage 8 doubt ?


# Solved errors
1. The header of the blocks of relcat and attrcat are messed up.
    - Issue was with getHeader which returned garbage value for blocktype as it was not initialized

# Errors
1. Crash in select while using targetrel as BigNumbersInd.
2. Wrong result in select
