import threading
import queue
from DrissionPage import Chromium
 
class TabWorker(threading.Thread):
    def __init__(self, browser, task_queue, result_queue):
        super().__init__()
        self.browser = browser
        self.task_queue = task_queue
        self.result_queue = result_queue
        
    def run(self):
        while True:
            try:
                url = self.task_queue.get(timeout=1)
                tab = self.browser.new_tab(url)
                
                # 执行具体任务
                data = self.scrape_data(tab)
                self.result_queue.put((url, data))
                
                tab.close()
                self.task_queue.task_done()
                
            except queue.Empty:
                break
    
    def scrape_data(self, tab):
        # 你的具体爬取逻辑
        elements = tab.eles('.item')
        return [ele.text for ele in elements]
 
if __name__ == "__main__":
    # 使用示例
    browser = Chromium()
    task_queue = queue.Queue()
    result_queue = queue.Queue()
    
    # 添加任务
    urls = ['url1', 'url2', 'url3', 'url4', 'url5']
    for url in urls:
        task_queue.put(url)
    
    # 启动工作线程
    workers = []
    for i in range(3):  # 3个线程同时工作
        worker = TabWorker(browser, task_queue, result_queue)
        worker.start()
        workers.append(worker)
    
    # 等待完成
    for worker in workers:
        worker.join()
    
    # 收集结果
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    browser.quit()