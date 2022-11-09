import concurrent.futures

import blog_sipder

# craw
with concurrent.futures.ThreadPoolExecutor() as pool:
    htmls = pool.map(blog_sipder.craw, blog_sipder.urls)
    htmls = list(zip(blog_sipder.urls, htmls))
    for url, html in htmls:
        print(f"{url}: {len(html)}")
print("craw done")


# parse
with concurrent.futures.ThreadPoolExecutor() as pool:
    futures = {}
    for url, html in htmls:
        future = pool.submit(blog_sipder.parse, html)
        futures[future] = url

    for future, url in futures.items():
        print(f"{url}: {future.result()}")
