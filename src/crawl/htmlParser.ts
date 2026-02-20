import { load } from "cheerio";

export interface ParsedPdfLink {
  url: string;
  title: string;
  year?: number;
  month?: string;
}

const MONTHS = [
  "january",
  "february",
  "march",
  "april",
  "may",
  "june",
  "july",
  "august",
  "september",
  "october",
  "november",
  "december",
] as const;

function sanitizeTitle(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function extractYearAndMonth(text: string): Pick<ParsedPdfLink, "year" | "month"> {
  const normalized = text.toLowerCase();
  const yearMatch = normalized.match(/\b(20\d{2}|19\d{2})\b/);
  const month = MONTHS.find((m) => normalized.includes(m));

  return {
    year: yearMatch ? Number.parseInt(yearMatch[1], 10) : undefined,
    month: month ? month[0].toUpperCase() + month.slice(1) : undefined,
  };
}

function normalizeUrl(baseUrl: string, href: string): string {
  return new URL(href, baseUrl).toString();
}

export function extractPdfLinksFromHtml(html: string, pageUrl: string): ParsedPdfLink[] {
  const $ = load(html);
  const links: ParsedPdfLink[] = [];
  const seen = new Set<string>();

  $("a[href$='.pdf'], a[href*='/wp-content/uploads/']").each((_, element) => {
    const href = $(element).attr("href");
    if (!href) {
      return;
    }

    const normalizedUrl = normalizeUrl(pageUrl, href);
    if (!normalizedUrl.toLowerCase().includes(".pdf")) {
      return;
    }

    if (seen.has(normalizedUrl)) {
      return;
    }

    const rawText = $(element).text() || $(element).attr("title") || $(element).closest("article, li, div").first().text();
    const title = sanitizeTitle(rawText || normalizedUrl.split("/").pop() || "Untitled PDF");
    const labels = extractYearAndMonth(`${title} ${$(element).closest("article, li, div").first().text()}`);
    seen.add(normalizedUrl);
    links.push({
      url: normalizedUrl,
      title,
      year: labels.year,
      month: labels.month,
    });
  });

  return links;
}

export function extractNextPageUrl(html: string, pageUrl: string): string | undefined {
  const $ = load(html);

  const explicitNext =
    $("a[rel='next']").attr("href") ||
    $(".pagination a.next").attr("href") ||
    $("nav.pagination a:contains('Next')").attr("href") ||
    $("a:contains('Older')").attr("href");

  if (explicitNext) {
    return normalizeUrl(pageUrl, explicitNext);
  }

  return undefined;
}
