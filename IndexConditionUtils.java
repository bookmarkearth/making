package com.bookmarkchina.module.search.util;

import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.bookmarkchina.base.util.MD5Util;
import com.bookmarkchina.base.util.URLUtil;
import com.bookmarkchina.base.util.lucene.LucenePrepareData;
import com.bookmarkchina.module.supervision.util.IllegalHelperUtil;

public class IndexConditionUtils {
	
	  public static boolean isHitted(List<String> list,LucenePrepareData prepareData,String folderName,int allowNum) throws SQLException{
		   try{
			    List<String> domainMd5List=new ArrayList<String>();
			    
			    if(list==null){return false;}
			    list.forEach(url->{
					try{
						 if(url.startsWith("http")){
							 String domain="";
							 try {domain = URLUtil.getDomainName(url);} catch (MalformedURLException e) {e.printStackTrace();}	
							 String domainMd5=MD5Util.encode2hex(domain);
							 domainMd5List.add(domainMd5);
						 }
					 }catch(Exception e){
						 e.printStackTrace();
					 }
			    });

			    if(IllegalHelperUtil.containsIllegalWords(folderName)!=null){return true;}
			    
			    String domainMd5Str=SearchToolUtil.listToStrForIn(domainMd5List);
			    Long illegalNum=prepareData.getTableDataFullAmount("illegal_url","domain_md5 in("+domainMd5Str+")","*");
			    if(illegalNum>allowNum){//allowNum条违规以上直接放弃索引
				 	 return true;
				}

		   }catch(Exception e){
			   e.printStackTrace();
		   }
		   return false; 
	   }
}
