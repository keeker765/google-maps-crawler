import re
import base64
from pathlib import Path
from DrissionPage import Chromium
from gmaps_crawler.pipeline.extractors.web_extractors.utils import extract_base64_links


def extract_emails_phones_socials(page: Chromium, websites: list) -> dict:
    """
    从多个网站提取邮箱、电话、社交媒体链接（含Base64解析）。
    自动过滤 URL 中的 '@'（如 Sentry DSN），避免误识别。
    返回:
    {
        "emails": [ {"email": str, "source_url": str}, ... ],
        "phones": [str],
        "socials": {平台: 最短URL或空字符串},
        "per_site": {
            网站URL: {
                "emails": [str],
                "phones": [str],
                "socials": {平台: 最短URL或空字符串}
            }
        }
    }
    """
    if not websites:
        return {"emails": [], "phones": [], "socials": {}, "per_site": {}}

    # 确保URL格式
    websites = [
        f"http://{url}" if not url.startswith(("http://", "https://")) else url
        for url in websites
    ]

    tab = page.new_tab()

    # 改进版正则（排除URL中的@）
    email_pattern = r'(?<![\/:@])\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b(?![\/:])'
    phone_pattern = r'\+\d[\d\s\-]{6,15}\d'
    url_pattern = r'https?://[^\s"\'<>]+'

    # 常见社交平台域名
    social_domains = {
        "facebook": ["facebook.com"],
        "instagram": ["instagram.com"],
        "twitter": ["twitter.com", "x.com"],
        "linkedin": ["linkedin.com"],
        "youtube": ["youtube.com", "youtu.be"],
        "tiktok": ["tiktok.com"],
        "whatsapp": ["whatsapp.com"],
        "telegram": ["t.me", "telegram.me"],
        "yelp": ["yelp.com", "yelp.fr", "yelp.ie", "yelp.co.uk"],
    }

    # 全局存储
    emails_with_source = {}
    all_phones = set()
    found_socials = {k: set() for k in social_domains}
    results_per_site = {}

    for url in websites:
        site_emails = set()
        site_phones = set()
        site_socials = {k: set() for k in social_domains}

        try:
            tab.get(url, timeout=15)
            html_content = tab.html

            # === 邮箱 ===
            for e in re.findall(email_pattern, html_content):
                e = e.lower()
                # 排除常见非邮箱用途的域（Sentry、AWS、Cloudflare等）
                if any(x in e for x in ['sentry.io', 'amazonaws.com', 'cloudflare.com']):
                    continue
                # 邮箱不应出现在URL内部
                if re.search(r'https?://[^"\'\s]*' + re.escape(e), html_content):
                    continue
                # 排除图片文件伪装的邮箱名
                if re.search(r'\.(jpg|jpeg|png|gif|svg|webp)$', e, re.IGNORECASE):
                    continue

                emails_with_source.setdefault(e, url)
                site_emails.add(e)

            # === 电话 ===
            for p in re.findall(phone_pattern, html_content):
                clean_p = re.sub(r'\s+', '', p)
                all_phones.add(clean_p)
                site_phones.add(clean_p)

            # === 普通社交链接 ===
            for link in re.findall(url_pattern, html_content):
                link_lower = link.lower()
                for platform, domains in social_domains.items():
                    if any(d in link_lower for d in domains):
                        found_socials[platform].add(link)
                        site_socials[platform].add(link)

            # === Base64隐藏链接 ===
            decoded_links = extract_base64_links(html_content)
            for d_link in decoded_links:
                for platform, domains in social_domains.items():
                    if any(d in d_link.lower() for d in domains):
                        found_socials[platform].add(d_link)
                        site_socials[platform].add(d_link)

        except Exception as e:
            print(f"⚠️ 访问 {url} 时出错: {e}")

        # 每个网站单独保存
        results_per_site[url] = {
            "emails": sorted(site_emails),
            "phones": sorted(site_phones),
            "socials": {p: (min(v, key=len) if v else "") for p, v in site_socials.items()},
        }

    tab.close()

    # 汇总结果
    socials_summary = {p: (min(v, key=len) if v else "") for p, v in found_socials.items()}
    email_list = [{"email": e, "source_url": src} for e, src in emails_with_source.items()]

    return {
        "emails": email_list,
        "phones": sorted(all_phones),
        "socials": socials_summary,
        "per_site": results_per_site,
    }



# =======================
# 测试部分
# =======================
if __name__ == "__main__":
    test_websites = [
        "https://cafecherie.fr",
        "https://www.yelp.ie/biz/cafe-cherie-boulogne",
        "https://www.facebook.com/Emilieandthecoolkids/",
        "https://www.privateaser.com/lieu/49030-cafe-cherie-brasserie-bar-a-cocktail",
    ]
    cp = Chromium(12312)
    result = extract_emails_phones_socials(cp, test_websites)
    print(result)

    print("\n📧 全部邮箱:")
    for e in result["emails"]:
        print(f"  - {e['email']} ({e['source_url']})")

    print("\n📞 电话:")
    for p in result["phones"]:
        print(f"  - {p}")

    print("\n🔗 社交媒体汇总:")
    for platform, link in result["socials"].items():
        if link:
            print(f"  {platform}: {link}")

    print("\n🌐 各网站详细结果:")
    for site, data in result["per_site"].items():
        print(f"\n{site}:")
        print(f"  Emails: {data['emails']}")
        print(f"  Phones: {data['phones']}")
        print(f"  Socials: {data['socials']}")
