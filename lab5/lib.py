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
