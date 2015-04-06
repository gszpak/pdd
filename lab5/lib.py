import random
import string


@outputSchema("b:bag{t:tuple(url:chararray,pagerank:double)}")
def top3(urls_with_ranks):
    urls = map(lambda (query, url, rank): (rank, url), list(urls_with_ranks))
    urls.sort(reverse=True)
    result = urls[:3]
    result = map(lambda (rank, url): (url, rank), result)
    return result


@outputSchema("t:tuple(url:chararray, revenue:int)")
def distributeRevenue(results, revenue):
    results = map(lambda(query, url, rank): (rank, url), results)
    results.sort()
    rev = {}
    for query, slot, amount in revenue:
        assert slot not in rev
        rev[slot] = amount
    out = []
    for rank, url in results:
        if rank <= 2 and 'top' in rev:
            out.append((url, rev['top']))
        elif rank > 2 and rank <= 5 and 'side' in rev:
            out.append((url, rev['side']))
        else:
            if 'bottom' in rev:
                out.append((url, rev['bottom']))
    return out


@outputSchema("b:bag{t:tuple(expandedQuery:chararray)}")
def expandQuery(query):
    
    def get_random_string(suffix_length=10):
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(suffix_length))

    result = []
    for _ in range(random.randint(1, 5)):
        result.append(query + ' ' + get_random_string())
    return result


@outputSchema("b:bag{t:tuple(category:chararray, pagerank:double)}")
def map_(category, url, rank):
    return (category, rank)


@outputSchema("t:tuple(pagerank: double)")
def reduce(category, ranks):
    return (max(ranks))
