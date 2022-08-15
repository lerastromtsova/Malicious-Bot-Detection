"""
Steps:
1. Collect user features:
    - FOAF XML ya:created
    - country
    - counters.followers
    - FOAF XML ya:subscribedToCount
    - comment rate: number of all comments by user in db
    - deactivated - not used as a feature but used in
      cluster analysis later on
2. Write user similarity function. Different for:
    - nominal data type
    - real data type
3. Construct multi-attributed graph Gm
4. Convert to similarity graph Gs using (2)
5. Construct similarity matrix from Gs
6. Apply Markov clustering to the matrix:
    - expansion
    - inflation
7. Analyse each cluster one by one
"""
