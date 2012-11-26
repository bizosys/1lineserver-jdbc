package com.oneline.dao;

public class ReadCsvProcessor {
	
	public static final int INITIAL_STRING_SIZE = 128;	
	private char separator = ',';
	private char escapechar;
	private String lineEnd = "\n";
    private char quotechar;
    /** The quote constant to use when you wish to suppress all quoting. */
    public static final char NO_QUOTE_CHARACTER = '\u0000';
    
    /** The escape constant to use when you wish to suppress all escaping. */
    public static final char NO_ESCAPE_CHARACTER = '\u0000';

    public ReadCsvProcessor(char separator) {
        this.separator = separator;
        this.quotechar = NO_QUOTE_CHARACTER;
        this.escapechar = NO_ESCAPE_CHARACTER;
    }

    public ReadCsvProcessor(char separator, char quotechar, char escapechar) {
        this.separator = separator;
        this.quotechar = quotechar;
        this.escapechar = escapechar;
    }    
	
    /**
     * Write a row.
     * @param cells
     * @param fillBuffer
     * @return
     */
    public void writeRow(String[] cells, StringBuilder fillBuffer) {
        	
        	if (cells == null) return;
        	
        	fillBuffer.delete(0, fillBuffer.capacity());
            for (int i = 0; i < cells.length; i++) {

                if (i != 0) {
                    fillBuffer.append(separator);
                }

                String nextElement = cells[i];
                if (nextElement == null)
                    continue;
                if (quotechar !=  NO_QUOTE_CHARACTER)
                	fillBuffer.append(quotechar);
                
                fillBuffer.append(hasSpecialCharacters(nextElement) ? escapeSpecialCharacters(nextElement) : nextElement);

                if (quotechar != NO_QUOTE_CHARACTER)
                	fillBuffer.append(quotechar);
            }
            
            fillBuffer.append(lineEnd);
        }    
    
	private boolean hasSpecialCharacters(String line) {
	    return line.indexOf(quotechar) != -1 || line.indexOf(escapechar) != -1;
    }
	
	private StringBuilder escapeSpecialCharacters(String nextElement)
    {
		StringBuilder sb = new StringBuilder(INITIAL_STRING_SIZE);
	    for (int j = 0; j < nextElement.length(); j++) {
	        char nextChar = nextElement.charAt(j);
	        if (escapechar != NO_ESCAPE_CHARACTER && nextChar == quotechar) {
	        	sb.append(escapechar).append(nextChar);
	        } else if (escapechar != NO_ESCAPE_CHARACTER && nextChar == escapechar) {
	        	sb.append(escapechar).append(nextChar);
	        } else {
	            sb.append(nextChar);
	        }
	    }
	    return sb;
    }	
}
