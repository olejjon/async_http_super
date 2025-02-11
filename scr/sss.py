import aiohttp
import asyncio
import aiofiles
import json
from bs4 import BeautifulSoup

# Ограничиваем количество одновременных запросов
MAX_CONCURRENT_REQUESTS = 5


async def fetch_url(session, url):
    """
    Асинхронно выполняет HTTP-запрос и возвращает результат.
    """
    try:
        async with session.get(url) as response:
            if response.status == 200:
                content_type = response.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    data = await response.json()
                    return url, {"type": "json", "content": data}
                elif "text/html" in content_type:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    title = soup.title.string if soup.title else "No title"
                    return url, {"type": "html", "content": {"title": title}}
                else:
                    print(f"Skipping {url}: Unsupported content type")
                    return url, None
            else:
                print(f"Skipping {url}: Status code {response.status}")
                return url, None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"Error fetching {url}: {e}")
        return url, None


async def worker(session, queue, out_f):
    """
    Воркер, который берет URL из очереди, выполняет запросы и записывает результаты в файл.
    """
    while True:
        url = await queue.get()  # Берем URL из очереди
        if url is None:
            # Сигнал завершения работы
            break
        url, content = await fetch_url(session, url)
        if content is not None:
            result = {"url": url, "content": content}
            await out_f.write(json.dumps(result) + "\n")  # Записываем результат сразу
        queue.task_done()  # Помечаем задачу как выполненную


async def fetch_urls(input_file, output_file):
    """
    Асинхронно обрабатывает список URL-адресов, используя очередь и пул воркеров.
    Результаты записываются в файл по мере их получения.
    """
    queue = asyncio.Queue()  # Создаем очередь

    async with aiohttp.ClientSession() as session:
        async with aiofiles.open(output_file, mode="w") as out_f:
            # Создаем пул воркеров
            workers = [
                asyncio.create_task(worker(session, queue, out_f))
                for _ in range(MAX_CONCURRENT_REQUESTS)
            ]

            # Читаем URL из файла и добавляем их в очередь
            async with aiofiles.open(input_file, mode="r") as f:
                async for line in f:
                    url = line.strip()
                    await queue.put(url)

            # Ожидаем завершения всех задач в очереди
            await queue.join()

            # Останавливаем воркеров
            for _ in range(MAX_CONCURRENT_REQUESTS):
                await queue.put(None)  # Сигнал завершения
            await asyncio.gather(*workers)  # Ожидаем завершения всех воркеров


async def main():
    input_file = "urls.txt"
    output_file = "results.jsonl"
    await fetch_urls(input_file, output_file)


if __name__ == "__main__":
    asyncio.run(main())