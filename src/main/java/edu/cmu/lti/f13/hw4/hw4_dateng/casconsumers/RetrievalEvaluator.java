package edu.cmu.lti.f13.hw4.hw4_dateng.casconsumers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.collection.CasConsumer_ImplBase;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.FSList;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceProcessException;
import org.apache.uima.util.ProcessTrace;

import edu.cmu.lti.f13.hw4.hw4_dateng.typesystems.Document;
import edu.cmu.lti.f13.hw4.hw4_dateng.typesystems.Token;
import edu.cmu.lti.f13.hw4.hw4_dateng.utils.Utils;


public class RetrievalEvaluator extends CasConsumer_ImplBase {

  public class EvaluateResult
  {
    int qid;
    int rel;
    int rank;
    String sentence;
    double score;
  }
	/** query id number **/
	public ArrayList<Integer> qIdList;
	/** query and text relevant values **/
	public ArrayList<Integer> relList;
	/** document content **/
	public ArrayList<String> strList;
	/** query token list **/
	public ArrayList<HashMap<String, Integer>> tokMapList;
	
		
	public void initialize() throws ResourceInitializationException {

		qIdList = new ArrayList<Integer>();
		relList = new ArrayList<Integer>();
		tokMapList = new ArrayList<HashMap<String, Integer>>();
		strList = new ArrayList<String>();
	}

	/**
	 * TODO :: 1. construct the global word dictionary 2. keep the word
	 * frequency for each sentence
	 */
	@Override
	public void processCas(CAS aCas) throws ResourceProcessException {

		JCas jcas;
		try {
			jcas =aCas.getJCas();
		} catch (CASException e) {
			throw new ResourceProcessException(e);
		}

		FSIterator it = jcas.getAnnotationIndex(Document.type).iterator();
		
		if (it.hasNext()) {
			Document doc = (Document) it.next();
			
			//Make sure that your previous annotators have populated this in CAS
			FSList fsTokenList = doc.getTokenList();
			ArrayList<Token> tokenList = Utils.fromFSListToCollection(fsTokenList, Token.class);
			HashMap<String, Integer> tokenMap = new HashMap<String, Integer>();
			for(Token tok: tokenList)
			{
			  tokenMap.put(tok.getText(), tok.getFrequency());
			}
			tokMapList.add(tokenMap);
			qIdList.add(doc.getQueryID());
			relList.add(doc.getRelevanceValue());
			strList.add(doc.getText());
		}
	}

	/**
	 * TODO 1. Compute Cosine Similarity and rank the retrieved sentences 2.
	 * Compute the MRR metric
	 */
	@Override
	public void collectionProcessComplete(ProcessTrace arg0)
			throws ResourceProcessException, IOException {

		super.collectionProcessComplete(arg0);

		System.out.println("Ranked result evaluated by consine similarity:");
		evaluateByCosineSimilarity();
		System.out.println("\n\nRanked result evaluated by Dice coefficient:");
    evaluateByDiceCoefficient();
    System.out.println("\n\nRanked result evaluated by Jaccard coefficient:");
    evaluateByJaccardCoefficient();
		
	}

	/** compute document scores using cosine similarity
   *  then print the sorted result and MMR 
   */
	private void evaluateByCosineSimilarity()
	{
	  ArrayList<EvaluateResult> resultList = new ArrayList<EvaluateResult>();
    // TODO :: compute the cosine similarity measure
    int qid;
    int rel;
    String docSten = "";
    HashMap<String, Integer> doctokenMap;
    HashMap<String, Integer> qryTokenMap;
    double score;
    for(int i=0; i<qIdList.size(); i++)
    {
      qid = qIdList.get(i);
      rel = relList.get(i);
      doctokenMap = tokMapList.get(i);
      docSten = strList.get(i);
      if(rel!=99)
      {
        qryTokenMap = tokMapList.get(getQrySentIndex(qid));
        //compare and get score
        score = computeCosineSimilarity(qryTokenMap, doctokenMap);
        
        EvaluateResult docResult = new EvaluateResult();
        docResult.qid = qid;
        docResult.rel = rel;
        docResult.score = score;
        docResult.sentence = docSten;
        resultList.add(docResult);
      }
    }
    sortAndPrintResult(resultList);
	}

	/** compute document scores using Dice coefficient
   *  then print the sorted result and MMR 
   */
	private void evaluateByDiceCoefficient()
  {
    ArrayList<EvaluateResult> resultList = new ArrayList<EvaluateResult>();
    // TODO :: compute the cosine similarity measure
    int qid;
    int rel;
    String docSten = "";
    HashMap<String, Integer> doctokenMap;
    HashMap<String, Integer> qryTokenMap;
    double score;
    for(int i=0; i<qIdList.size(); i++)
    {
      qid = qIdList.get(i);
      rel = relList.get(i);
      doctokenMap = tokMapList.get(i);
      docSten = strList.get(i);
      if(rel!=99)
      {
        qryTokenMap = tokMapList.get(getQrySentIndex(qid));
        //compare and get score
        score = computeDiceCoefficient(qryTokenMap, doctokenMap);
        
        EvaluateResult docResult = new EvaluateResult();
        docResult.qid = qid;
        docResult.rel = rel;
        docResult.score = score;
        docResult.sentence = docSten;
        resultList.add(docResult);
      }
    }
    sortAndPrintResult(resultList);
  }
	
	/** compute document scores using Jaccard coefficient
	 *  then print the sorted result and MMR 
	 */
	private void evaluateByJaccardCoefficient()
  {
    ArrayList<EvaluateResult> resultList = new ArrayList<EvaluateResult>();
    // TODO :: compute the cosine similarity measure
    int qid;
    int rel;
    String docSten = "";
    HashMap<String, Integer> doctokenMap;
    HashMap<String, Integer> qryTokenMap;
    double score;
    for(int i=0; i<qIdList.size(); i++)
    {
      qid = qIdList.get(i);
      rel = relList.get(i);
      doctokenMap = tokMapList.get(i);
      docSten = strList.get(i);
      if(rel!=99)
      {
        qryTokenMap = tokMapList.get(getQrySentIndex(qid));
        //compare and get score
        score = computeJaccardCoefficient(qryTokenMap, doctokenMap);
        
        EvaluateResult docResult = new EvaluateResult();
        docResult.qid = qid;
        docResult.rel = rel;
        docResult.score = score;
        docResult.sentence = docSten;
        resultList.add(docResult);
      }
    }
    sortAndPrintResult(resultList);
  }
	
	/** compute the rank of retrieved sentences, then print the ranked results and MRR matrix
	 * @param resultList
	 */
	private void sortAndPrintResult(ArrayList<EvaluateResult> resultList)
	{
	// TODO :: compute the rank of retrieved sentences
    Comparator<EvaluateResult> comparator = new Comparator<EvaluateResult>() {
      public int compare(EvaluateResult r1, EvaluateResult r2)
      {
        if(r1.qid>r2.qid)
        {
          return 1;
        }
        else if(r1.qid==r2.qid)
        {
          if(r1.score<r2.score)
            return 1;
          else
            return 0;
        }
        else
        {
          return 0;
        }
      }
    };
    Collections.sort(resultList, comparator);
    
    int current_qid = resultList.get(0).qid;
    int docRank = 0;
    for(EvaluateResult er: resultList)
    {
      if(er.qid != current_qid)
      {
        current_qid = er.qid;
        docRank = 1;
      }
      else
      {
        docRank++;
      }
      er.rank = docRank;
      if(er.rel==1)
      {
        System.out.print("Score: ");
        System.out.printf("%.10f",er.score);
        System.out.println("  rank=" + er.rank + "  rel="+er.rel+" qid="+er.qid +" "+er.sentence);
       }
    }
    // TODO :: compute the metric:: mean reciprocal rank
    double metric_mrr = compute_mrr(resultList);
    System.out.println(" (MRR) Mean Reciprocal Rank ::" + metric_mrr);
	}
	
	/**TODO :: compute Dice coefficient between two sentences
	 * @param queryVector
	 * @param docVector
	 * @return
	 */
	private double computeDiceCoefficient (Map<String, Integer> queryVector, Map<String, Integer> docVector)
	{
	  double common = 0;

    Iterator<Entry<String, Integer>> qryIt = queryVector.entrySet().iterator();
    Iterator<Entry<String, Integer>> docIt = docVector.entrySet().iterator();
    
    while(qryIt.hasNext())
    {
      Entry<String, Integer> qryTok = qryIt.next();
      while(docIt.hasNext())
      {
        Entry<String, Integer> docTok = docIt.next();
        if(qryTok.getKey().equals(docTok.getKey()))
        {
          common++;
        }
      }
      docIt = docVector.entrySet().iterator();
    }
    return 2*common/(queryVector.size()+docVector.size());
	}
	
	/** compute Jaccard coefficient between two sentences
	 * @param queryVector
	 * @param docVector
	 * @return
	 */
	private double computeJaccardCoefficient(Map<String, Integer> queryVector, Map<String, Integer> docVector)
  {
    double intersection = 0;
    double union = 0;
    
    Iterator<Entry<String, Integer>> qryIt = queryVector.entrySet().iterator();
    Iterator<Entry<String, Integer>> docIt = docVector.entrySet().iterator();
    
    while(qryIt.hasNext())
    {
      Entry<String, Integer> qryTok = qryIt.next();
      while(docIt.hasNext())
      {
        Entry<String, Integer> docTok = docIt.next();
        if(qryTok.getKey().equals(docTok.getKey()))
        {
          intersection++;
        }
      }
      docIt = docVector.entrySet().iterator();
    }
    union = queryVector.size()+docVector.size() - intersection;
    return intersection/union;
  }
	
	
	/** compute cosine similarity between two sentences
	 * @param queryVector
	 * @param docVector
	 * @return
	 */
	private double computeCosineSimilarity(Map<String, Integer> queryVector, Map<String, Integer> docVector) {
		double cosine_similarity=0.0;

		double dotPro = 0;
		double qryMag = 0;
		double docMag = 0;
		// TODO :: compute cosine similarity between two sentences
		Iterator<Entry<String, Integer>> qryIt = queryVector.entrySet().iterator();
		Iterator<Entry<String, Integer>> docIt = docVector.entrySet().iterator();
		
		while(qryIt.hasNext())
		{
		  Entry<String, Integer> qryTok = qryIt.next();
		  while(docIt.hasNext())
		  {
		    Entry<String, Integer> docTok = docIt.next();
		    if(qryTok.getKey().equals(docTok.getKey()))
		    {
		      dotPro += qryTok.getValue()*docTok.getValue();
		    }
		  }
		  docIt = docVector.entrySet().iterator();
		}
		//compute query magnitude		
		qryIt = queryVector.entrySet().iterator();
    
		while(qryIt.hasNext())
		{
		  qryMag += Math.pow(qryIt.next().getValue(), 2);
		}
		qryMag = Math.pow(qryMag, 0.5);
		
		//compute document magnitude
		docIt = docVector.entrySet().iterator();
		while(docIt.hasNext())
    {
		  docMag += Math.pow(docIt.next().getValue(), 2);
    }
		docMag = Math.pow(docMag, 0.5);
		
		cosine_similarity = dotPro/(qryMag*docMag);
		return cosine_similarity;
	}

	/**
	 * compute Mean Reciprocal Rank of the given evaluated result
	 * @return mrr
	 */
	private double compute_mrr(ArrayList<EvaluateResult> resultList) {
		double metric_mrr=0.0;
		double sum_reciprocalRank = 0;
		
		// TODO :: compute Mean Reciprocal Rank (MRR) of the text collection
		int current_qid = resultList.get(0).qid;
		int start_pos = 0;
		int totalQry = 0;
		for(int i=0; i<resultList.size(); )
		{
		  if(resultList.get(i).rel ==1)
		  {
		    totalQry++;
		    sum_reciprocalRank += 1/(double)(i-start_pos+1);
		    while(i<resultList.size() && resultList.get(i).qid==current_qid)
		      i++;
		    if(i<resultList.size())
		    {
		      current_qid = resultList.get(i).qid;
	        start_pos = i;
		    }
		  }
		  else
		    i++;
		}
		return sum_reciprocalRank/totalQry;
	}
	
	/**
	 * Get the index of the query which Id is qryQId in the qIdList
	 * @param qryQId
	 * @return
	 */
	private int getQrySentIndex(int qryQId)
  {
    int i;
    for(i=0; i<qIdList.size(); i++)
    {
      if(qIdList.get(i).intValue()==qryQId && relList.get(i).intValue()==99)
      {
        return i;
      }
    }
    return -1;
  }
  
}
