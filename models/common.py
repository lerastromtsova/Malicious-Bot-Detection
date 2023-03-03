def get_jaccard_similarity(vec1, vec2):
    v1, v2 = set(vec1), set(vec2)
    if v1 and v2:
        return len(v1.intersection(v2)) / len(v1.union(v2))
    return 0


def get_weighted_edge(user1, user2, images):
    user1_vector = []
    user2_vector = []
    for comment in images[user1]:
        for vk_id, imgs in comment.items():
            for i in imgs:
                user1_vector.append(i)
    for comment in images[user2]:
        for vk_id, imgs in comment.items():
            for i in imgs:
                user2_vector.append(i)
    return user1, user2, get_jaccard_similarity(user1_vector, user2_vector)
