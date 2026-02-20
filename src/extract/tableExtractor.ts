export interface TableExtractionOutput {
  location?: string;
  note?: string;
}

export interface TableExtractor {
  extractTables(docId: string, rawPdfPath: string): Promise<TableExtractionOutput>;
}
