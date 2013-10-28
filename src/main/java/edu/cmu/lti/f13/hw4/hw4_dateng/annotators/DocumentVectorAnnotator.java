package edu.cmu.lti.f13.hw4.hw4_dateng.annotators;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Vector;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.FSList;
import org.apache.uima.jcas.tcas.Annotation;

import edu.cmu.lti.f13.hw4.hw4_dateng.EnglishAnalyzerConfigurable;
import edu.cmu.lti.f13.hw4.hw4_dateng.typesystems.Document;
import edu.cmu.lti.f13.hw4.hw4_dateng.typesystems.Token;
import edu.cmu.lti.f13.hw4.hw4_dateng.utils.Utils;

public class DocumentVectorAnnotator extends JCasAnnotator_ImplBase {

  /**
   * This is an external class that help to do stemming and stopwords removal on the query tokens 
   * **/
  public static EnglishAnalyzerConfigurable analyzer = new EnglishAnalyzerConfigurable(Version.LUCENE_43);
  static {
    analyzer.setLowercase(true);
    analyzer.setStopwordRemoval(true);
    analyzer.setStemmer(EnglishAnalyzerConfigurable.StemmerType.KSTEM);
  }
  
	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {

		FSIterator<Annotation> iter = jcas.getAnnotationIndex().iterator();
		if (iter.isValid()) {
			iter.moveToNext();
			Document doc = (Document) iter.get();
			try {
        createTermFreqVector(jcas, doc);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
		}
		
	}
	/**
	 * Get tokens from a doc. Perform stemming and stopwords removal on each token.
	 * And add the tokens to the tokenList as a document's attribute
	 * @param jcas
	 * @param doc
	 * @throws IOException 
	 */
	private void createTermFreqVector(JCas jcas, Document doc) throws IOException {

		String docText = doc.getText();
		HashMap<String, Integer> tokenMap = new HashMap<String, Integer>();  
		Vector<Token> tokenVector = new Vector<Token>();
		//TO DO: construct a vector of tokens and update the tokenList in CAS
		String [] tokens  = docText.split("[\t ]");
		for(String token: tokens)
		{
		  String[] stemedTok = stemToken(token);
		  if(stemedTok.length!=0)
		  {
		    token = stemedTok[0];
  		  if(tokenMap.containsKey(token))
  		  {
  		    tokenMap.put(token, tokenMap.get(token).intValue()+1);
  		  }
  		  else
  		  {
  		    tokenMap.put(token, 1);
  		  }
		  }
		}
		Iterator<Entry<String,Integer>> it = tokenMap.entrySet().iterator();
		Entry<String,Integer> entry;
		Token tok;
		while(it.hasNext())
		{
		  entry = it.next();
		  tok = new Token(jcas);
		  tok.setText(entry.getKey());
		  tok.setFrequency(entry.getValue().intValue());
		  tokenVector.add(tok);
		  tok.addToIndexes();
		}
		FSList fsList = Utils.fromCollectionToFSList(jcas, tokenVector);
		doc.setTokenList(fsList);
	}

	/**
   * Given a query string, returns the terms one at a time with stopwords removed and the terms
   * stemmed using the Krovetz stemmer.
   * 
   * Use this method to process raw query terms.
   * 
   * @param query
   *          String containing query
   * @return Array of query tokens
   * @throws IOException
   */
  static String[] stemToken(String query) throws IOException {

    TokenStreamComponents comp = analyzer.createComponents("dummy", new StringReader(query));
    TokenStream tokenStream = comp.getTokenStream();

    CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();

    List<String> tokens = new ArrayList<String>();
    while (tokenStream.incrementToken()) {
      String term = charTermAttribute.toString();
      tokens.add(term);
    }
    return tokens.toArray(new String[tokens.size()]);
  }
}
