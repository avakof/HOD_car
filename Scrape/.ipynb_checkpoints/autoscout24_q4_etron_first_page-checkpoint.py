import json
import re
from urllib.parse import urljoin

import pandas as pd
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


# Block 3 — Configuration and constants
SEARCH_URL = (
    "https://www.autoscout24.com/lst/audi/q4-e-tron"
    "?atype=C&cy=D&damaged_listing=exclude&desc=1&ocs_listing=include"
    "&powertype=kw&search_id=wmvs3ql6fl&sort=age&source=homepage_search-mask"
    "&ustate=N%2CU"
)
BASE_URL = "https://www.autoscout24.com"
OUTPUT_CSV = "autoscout24_q4_etron_first_page.csv"

# Set HEADLESS = False temporarily while debugging in a visible browser.
HEADLESS = True
PAGE_TIMEOUT_MS = 45_000
POST_COOKIE_WAIT_MS = 1_500
NETWORK_IDLE_TIMEOUT_MS = 5_000
DEBUG_SAMPLE_RECORDS = 2

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

COLUMN_ORDER = [
    "title",
    "subtitle",
    "price",
    "currency",
    "mileage",
    "first_registration",
    "fuel_or_powertrain",
    "power_kw",
    "power_hp",
    "seller_name",
    "seller_location",
    "image_count",
    "vat_deductible",
    "delivery_possible",
    "listing_url",
    "raw_card_text",
]


# Block 4 — Selector definitions
SELECTORS = {
    "cookie_accept_buttons": [
        "button[data-testid='as24-cmp-accept-all-button']",
        "button[data-testid='as24-cmp-decline-all-button']",
        "button:has-text('Accept All')",
    ],
    "next_data_script": "#__NEXT_DATA__",
    "offer_links": "a[href*='/offers/']",
}

TEXT_MARKERS = {
    "vat_deductible": [
        "VAT deductible",
        "VAT deduct.",
        "MwSt. ausweisbar",
        "TVA déductible",
        "IVA deducibile",
    ],
    "delivery_possible": [
        "Delivery possible",
        "Nationwide delivery",
        "Home delivery",
    ],
}


# Block 5 — Helper functions for text cleaning and safe extraction
def clean_text(value):
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def first_non_empty(*values):
    for value in values:
        cleaned = clean_text(value)
        if cleaned is not None:
            return cleaned
    return None


def make_absolute_url(url):
    cleaned = clean_text(url)
    if cleaned is None:
        return None
    return urljoin(BASE_URL, cleaned)


def split_price_and_currency(price_text):
    price_text = clean_text(price_text)
    if price_text is None:
        return None, None

    currency_match = re.match(r"^\D+", price_text)
    currency = clean_text(currency_match.group(0)) if currency_match else None
    numeric_price = clean_text(price_text.replace(currency or "", "", 1))
    return numeric_price, currency


def parse_power_values(power_text):
    power_text = clean_text(power_text)
    if power_text is None:
        return None, None

    kw_match = re.search(r"([\d.,]+)\s*kW", power_text, flags=re.IGNORECASE)
    hp_match = re.search(r"([\d.,]+)\s*hp", power_text, flags=re.IGNORECASE)

    power_kw = clean_text(kw_match.group(1)) if kw_match else None
    power_hp = clean_text(hp_match.group(1)) if hp_match else None
    return power_kw, power_hp


def build_detail_map(listing_data):
    detail_map = {}
    for item in listing_data.get("vehicleDetails") or []:
        label = clean_text(item.get("ariaLabel"))
        value = clean_text(item.get("data"))
        if label and value:
            detail_map[label] = value
    return detail_map


def build_title(listing_data, dom_title=None):
    dom_title = clean_text(dom_title)
    if dom_title:
        return dom_title

    vehicle = listing_data.get("vehicle") or {}
    title_parts = [
        clean_text(vehicle.get("make")),
        clean_text(vehicle.get("model")),
        clean_text(vehicle.get("modelVersionInput")),
    ]
    title_parts = [part for part in title_parts if part]
    return clean_text(" ".join(title_parts)) or clean_text(vehicle.get("variant"))


def build_seller_location(location_data):
    location_data = location_data or {}
    country_code = clean_text(location_data.get("countryCode"))
    zip_code = clean_text(location_data.get("zip"))
    city = clean_text(location_data.get("city"))

    location_parts = []
    if country_code and zip_code:
        location_parts.append(f"{country_code}-{zip_code}")
    elif country_code:
        location_parts.append(country_code)
    elif zip_code:
        location_parts.append(zip_code)

    if city:
        location_parts.append(city)

    return clean_text(" ".join(location_parts))


def marker_present(raw_text, markers):
    raw_text = clean_text(raw_text)
    if raw_text is None:
        return None

    lowered = raw_text.lower()
    return True if any(marker.lower() in lowered for marker in markers) else None


def empty_record(listing_url=None, raw_card_text=None):
    record = {column: None for column in COLUMN_ORDER}
    record["listing_url"] = make_absolute_url(listing_url)
    record["raw_card_text"] = clean_text(raw_card_text)
    return record


def load_listings_from_next_data(page):
    raw_json = page.locator(SELECTORS["next_data_script"]).inner_text()
    data = json.loads(raw_json)
    listings = data["props"]["pageProps"].get("listings") or []
    return listings


def print_debug_sample(card_payloads, reason):
    sample = next((item for item in card_payloads.values() if item.get("raw_card_text")), None)
    if sample is None:
        print(f"[debug] {reason}: no sample card text was captured.")
        return

    print(f"[debug] {reason}: sample raw card text")
    print(sample["raw_card_text"][:1_000])
    print(f"[debug] {reason}: sample outer HTML")
    print((sample.get("raw_card_html") or "")[:2_000])


# Block 6 — Browser launch
def launch_browser(playwright):
    browser = playwright.chromium.launch(headless=HEADLESS)
    context = browser.new_context(
        user_agent=USER_AGENT,
        locale="en-GB",
        viewport={"width": 1440, "height": 2200},
    )
    page = context.new_page()
    page.set_default_timeout(PAGE_TIMEOUT_MS)
    return browser, context, page


# Block 7 — Open page and handle cookie consent
def open_page_and_handle_cookies(page):
    page.goto(SEARCH_URL, wait_until="domcontentloaded")

    for selector in SELECTORS["cookie_accept_buttons"]:
        button = page.locator(selector)
        if button.count() == 0:
            continue

        try:
            button.first.click(timeout=3_000)
            print(f"Cookie banner handled with selector: {selector}")
            page.wait_for_timeout(POST_COOKIE_WAIT_MS)
            break
        except PlaywrightTimeoutError:
            continue

    try:
        page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        # AutoScout24 may keep background requests alive; this is best effort only.
        pass


# Block 8 — Wait for first-page listings to load
def wait_for_first_page_listings_to_load(page):
    page.wait_for_selector(SELECTORS["next_data_script"], state="attached")
    page.wait_for_selector(SELECTORS["offer_links"], state="attached")

    listings = load_listings_from_next_data(page)
    if not listings:
        raise RuntimeError("No listings were found in __NEXT_DATA__ on the first page.")

    print(f"Found {len(listings)} listings in __NEXT_DATA__ for the first page.")
    return listings


# Block 9 — Extract listing card elements
def extract_listing_card_elements(page, listings):
    listing_paths = [listing.get("url") for listing in listings if clean_text(listing.get("url"))]
    offer_link_count = page.locator(SELECTORS["offer_links"]).count()
    print(f"Found {offer_link_count} DOM offer links on the page.")

    card_payloads = page.evaluate(
        """
        (listingPaths) => {
          function findCard(anchor) {
            let current = anchor;
            for (let depth = 0; current && depth < 10; depth += 1, current = current.parentElement) {
              const text = (current.innerText || "").trim();
              if (!text) continue;

              const hasOfferLink = !!current.querySelector('a[href*="/offers/"]');
              const looksLikeCard = /€|\\bkm\\b|\\bkW\\b|\\bhp\\b/i.test(text);

              if (hasOfferLink && looksLikeCard && text.length >= 40) {
                return current;
              }
            }

            return anchor.closest("article, section") || anchor.parentElement || anchor;
          }

          return listingPaths.map((listingPath) => {
            const absoluteUrl = new URL(listingPath, document.baseURI).href;
            const selectors = [
              `a[href="${listingPath}"]`,
              `a[href="${absoluteUrl}"]`,
              `a[href$="${listingPath}"]`,
            ];

            let anchor = null;
            for (const selector of selectors) {
              anchor = document.querySelector(selector);
              if (anchor) break;
            }

            if (!anchor) {
              return {
                listing_path: listingPath,
                found: false,
                dom_title: null,
                raw_card_text: null,
                raw_card_html: null,
              };
            }

            const card = findCard(anchor);
            const heading = card.querySelector("h1, h2, h3");

            return {
              listing_path: listingPath,
              found: true,
              dom_title: (heading?.textContent || anchor.textContent || "").trim() || null,
              raw_card_text: (card.innerText || "").trim() || null,
              raw_card_html: (card.outerHTML || "").trim().slice(0, 8000) || null,
            };
          });
        }
        """,
        listing_paths,
    )

    payload_by_path = {item["listing_path"]: item for item in card_payloads}
    matched_cards = sum(1 for item in card_payloads if item.get("found"))
    print(f"Matched {matched_cards}/{len(listing_paths)} DOM cards to first-page listings.")
    return payload_by_path


# Block 10 — Parse one listing card into a dictionary
def parse_one_listing_card(listing_data, card_payload):
    vehicle = listing_data.get("vehicle") or {}
    seller = listing_data.get("seller") or {}
    detail_map = build_detail_map(listing_data)
    raw_card_text = clean_text((card_payload or {}).get("raw_card_text"))

    title = build_title(listing_data, dom_title=(card_payload or {}).get("dom_title"))
    subtitle = first_non_empty(
        vehicle.get("subtitle"),
        vehicle.get("modelVersionInput"),
        vehicle.get("variant"),
    )

    price_text = first_non_empty(
        (listing_data.get("price") or {}).get("priceFormatted"),
    )
    price, currency = split_price_and_currency(price_text)

    power_text = first_non_empty(
        detail_map.get("Power"),
        raw_card_text,
    )
    power_kw, power_hp = parse_power_values(power_text)

    delivery_hint = clean_text(
        (((seller.get("dealer") or {}).get("nationwideListingsData") or {}).get("consumerHint"))
    )
    if delivery_hint:
        delivery_possible = True
    else:
        delivery_possible = marker_present(raw_card_text, TEXT_MARKERS["delivery_possible"])

    vat_deductible = marker_present(raw_card_text, TEXT_MARKERS["vat_deductible"])

    return {
        "title": title,
        "subtitle": subtitle,
        "price": price,
        "currency": currency,
        "mileage": first_non_empty(detail_map.get("Mileage"), vehicle.get("mileageInKm")),
        "first_registration": first_non_empty(
            detail_map.get("First registration"),
            detail_map.get("Registration date"),
        ),
        "fuel_or_powertrain": first_non_empty(detail_map.get("Fuel type"), vehicle.get("fuel")),
        "power_kw": power_kw,
        "power_hp": power_hp,
        "seller_name": first_non_empty(seller.get("companyName"), seller.get("contactName")),
        "seller_location": build_seller_location(listing_data.get("location")),
        "image_count": len(listing_data.get("images") or []),
        "vat_deductible": vat_deductible,
        "delivery_possible": delivery_possible,
        "listing_url": make_absolute_url(listing_data.get("url")),
        "raw_card_text": raw_card_text,
    }


# Block 11 — Parse all cards into records
def parse_all_cards_into_records(listings, card_payload_by_path):
    records = []

    for listing in listings:
        listing_path = listing.get("url")
        card_payload = card_payload_by_path.get(listing_path, {})

        try:
            record = parse_one_listing_card(listing, card_payload)
        except Exception as exc:
            print(f"Failed to parse listing {listing_path}: {exc}")
            print_debug_sample({listing_path: card_payload}, reason="parse failure")
            record = empty_record(
                listing_url=listing_path,
                raw_card_text=card_payload.get("raw_card_text"),
            )

        records.append(record)

    print(f"Parsed {len(records)} records from the first page.")
    return records


# Block 12 — Create DataFrame with fixed column order
def create_dataframe(records):
    dataframe = pd.DataFrame(records)
    dataframe = dataframe.reindex(columns=COLUMN_ORDER)
    return dataframe


# Block 13 — Clean and normalize fields
def clean_and_normalize_dataframe(dataframe):
    dataframe = dataframe.copy()

    for column in dataframe.columns:
        if column == "image_count":
            continue
        dataframe[column] = dataframe[column].apply(clean_text)

    dataframe["image_count"] = pd.to_numeric(dataframe["image_count"], errors="coerce").astype("Int64")
    dataframe = dataframe.where(pd.notna(dataframe), None)
    return dataframe


# Block 14 — Save to CSV
def save_to_csv(dataframe, output_path):
    dataframe.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Saved {len(dataframe)} rows to {output_path}")


# Block 15 — Main execution block
def main():
    with sync_playwright() as playwright:
        browser, context, page = launch_browser(playwright)
        try:
            open_page_and_handle_cookies(page)
            listings = wait_for_first_page_listings_to_load(page)
            card_payload_by_path = extract_listing_card_elements(page, listings)

            if not any(payload.get("found") for payload in card_payload_by_path.values()):
                print_debug_sample(card_payload_by_path, reason="no DOM cards matched")

            records = parse_all_cards_into_records(listings, card_payload_by_path)

            if records:
                preview = records[:DEBUG_SAMPLE_RECORDS]
                print("Sample parsed records:")
                print(json.dumps(preview, indent=2, ensure_ascii=False))
            else:
                print_debug_sample(card_payload_by_path, reason="no records parsed")

            dataframe = create_dataframe(records)
            dataframe = clean_and_normalize_dataframe(dataframe)
            save_to_csv(dataframe, OUTPUT_CSV)
        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    main()


# Block 16 — Debugging tips and selector maintenance notes
# 1. Flip HEADLESS to False near the top of the file when you want to watch
#    the browser and validate the cookie banner or page timing.
# 2. If the site stops matching cards, check SELECTORS first, then inspect the
#    card_payload_by_path debug output from print_debug_sample().
# 3. The most stable source on this page is __NEXT_DATA__; if HTML classes move,
#    keep the JSON parsing and only adjust the DOM offer-link lookup logic.
# 4. The DOM card matching currently depends on a[href*="/offers/"] plus a
#    nearest-ancestor heuristic. That is the most likely part to need updates
#    if AutoScout24 changes its search result markup.
