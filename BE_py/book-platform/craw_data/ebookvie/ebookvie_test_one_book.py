from ebookvie_full_crawl import get_book_detail  # nếu file tên là vậy

if __name__ == "__main__":
    # hoặc 1 URL bất kỳ từ sitemap
    url = "https://ebookvie.com/ebook/logic-cua-tam-tri/"
    data = get_book_detail(url)
    for k, v in data.items():
        print(k, ":", v if len(str(v)) < 200 else str(v)[:200] + "...")
