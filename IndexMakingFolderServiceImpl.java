package com.bookmarkchina.module.search.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.bookmarkchina.base.BookmarkMakeBasicService;
import com.bookmarkchina.base.bean.mysql.BookmarkFolder;
import com.bookmarkchina.base.constant.CacheConstant;
import com.bookmarkchina.base.constant.Constant;
import com.bookmarkchina.base.util.lucene.LucenePrepareData;
import com.bookmarkchina.module.search.bean.QueryCondition;
import com.bookmarkchina.module.search.service.IndexMakingFolderService;
import com.bookmarkchina.module.search.util.CreateFolderIndex;
import com.bookmarkchina.module.search.util.IndexConditionUtils;

@Service
public class IndexMakingFolderServiceImpl extends BookmarkMakeBasicService implements IndexMakingFolderService{
	
	private Logger logger = Logger.getLogger(IndexMakingFolderServiceImpl.class);

	private final String indexCondition=null;
	private final String groupBy=null;
    private final Integer pieces=1000; 
	private CreateFolderIndex createIndex=new CreateFolderIndex();
	private LucenePrepareData prepareData=null;
	private static String keyTemplate=CacheConstant.w_m_k_v;

    public void indexInit(){//数据库+lcene初始化
    	createIndex.indexInit(Constant.DISC_URL_MAKING);
    }
   
    public List<BookmarkFolder> prepareIndexList(ResultSet rs) throws SQLException{
   	
	   	List<BookmarkFolder> list=new ArrayList<BookmarkFolder>();
	   	while(rs.next()){
	   		try{
	   			try{
	   				BookmarkFolder  bookmarkFolder= new BookmarkFolder();

	   				//校验文件夹内容，过滤重复
	   				long id=rs.getLong("id");
	   				String folder=rs.getString("folder");
	   				
	   				ResultSet rs1=prepareData.getFodlerContent(id,"u.name,u.url");
	   				Set<String> filterSet=new TreeSet<String>();//Treeset 自动排序，所以用他
	   				List<String> urlList = new ArrayList<String>();
	   				
	   				while(rs1.next()){
	   					filterSet.add(rs1.getString("name"));
	   					urlList.add(rs1.getString("url"));
	   				}
	   				
	   				//文件夹是否有风险，包括：色情、政治、赌博、版权
	   				Boolean illegal=IndexConditionUtils.isHitted(urlList,prepareData,folder,3);
	   				
	   				Boolean isSame=analysisFolderSimilarity(keyTemplate,filterSet);
	   				if(!isSame && !illegal){
	   					
						int bookmarkNum=rs.getInt("bookmark_num");
						Long bookmarkId=rs.getLong("bookmark_id");

						bookmarkFolder.setId(id);
						bookmarkFolder.setBookmarkId(bookmarkId);
						bookmarkFolder.setFolder(folder);
						bookmarkFolder.setBookmarkNum(bookmarkNum);

						list.add(bookmarkFolder);
	   				}
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
		} 
	   	prepareData.commitToDatabase();//最后commit一次
	   	return list;
   }
   
  public List<BookmarkFolder> getSourceListUnindex(QueryCondition query,String tableName,int indexFlag){
   	
   	try {
		ResultSet rs=prepareData.getTableDataUnindex(query.getStart(),query.getPieces(),tableName,indexFlag,indexCondition,groupBy);
		return prepareIndexList(rs);
	} catch (SQLException e) {
		e.printStackTrace();
	}
   	return null;
  }
   
  public void indexTemplateUnindex(QueryCondition query,String tableName,int indexFlag){
	  	
	  	List<BookmarkFolder> list=getSourceListUnindex(query,tableName,indexFlag);
		for(int i=0;i<list.size();i++){
			BookmarkFolder bookmarkFolder=list.get(i);
			createIndex.updateIndex(bookmarkFolder);
		}
		list=null;
  }
   
  @Override
  public void updateIncreasedIndex(String tableName,String serverName, int indexFlag){
	  	try{
	  		
	  		prepareData=new LucenePrepareData("id,bookmark_id,folder,bookmark_num");
	  		
	   		//初始化
	   		indexInit();
	   		prepareData.initDatebase(serverName);
	   		
	   		Long amount=prepareData.getTableDataPartAmount(tableName,indexFlag,indexCondition,"*");
	   		System.out.println("total amount is："+amount);
	   		QueryCondition query=new QueryCondition();
	   		Long start=0L;
	   		query.setPieces(pieces);
	   		while(start<=amount){
	   			query.setStart(0L);
	   			indexTemplateUnindex(query,tableName,indexFlag);
	   			start=start+pieces;
	   			createIndex.indexCommit();
	   			prepareData.updateTableDataIndex(tableName,indexFlag,pieces,indexCondition);//标记为已经索引
	   			System.out.println("commit "+start+" data to index block.");
	   		}
	   		logger.info("total "+amount+" data has commited, from table "+tableName);
	   		System.out.println("total "+amount+" data has commited, from table "+tableName);
	   	}
	   	catch(Exception e){
	   		e.printStackTrace();
	   	}
	   	finally{
	   		indexEnd();
	   		prepareData.endDatebase();
	   	}
  	}
  
  	public List<BookmarkFolder> getSourceListFull(QueryCondition query,String tableName){
	  	try {
			ResultSet rs=prepareData.getTableDataFull(query.getStart(),query.getPieces(),tableName,indexCondition,groupBy);
			return prepareIndexList(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	  	return null;
  	}
  
  	public void indexDocumentFull(QueryCondition query,String tableName){
  		List<BookmarkFolder> list=getSourceListFull(query,tableName);
		for(int i=0;i<list.size();i++){
			BookmarkFolder bookmarkFolder=list.get(i);
			createIndex.updateIndex(bookmarkFolder);
		}
		list=null;
  	}
  
  	@Override
	public void updateFullIndex(String tableName,String serverName){
	  	try{
	  		
	  		prepareData=new LucenePrepareData("id,bookmark_id,folder,bookmark_num");
	  		
	  		//初始化
	  		indexInit();
	  		prepareData.initDatebase(serverName);
	  		
	  		Long amount=prepareData.getTableDataFullAmount(tableName,indexCondition,"*");
	  		System.out.println("total amount is："+amount);
	  		QueryCondition query=new QueryCondition();
	  		Long start=0L;
	  		query.setPieces(pieces);
	  		while(start<=amount){
	  			query.setStart(start);
	  			indexDocumentFull(query,tableName);
	  			start=start+pieces;
	  			createIndex.indexCommit();
	  			System.out.println("commit "+start+" data to index block.");
	  		}
	  		System.out.println("total "+amount+" data has commited, from table "+tableName);
	  		
	  	}
	  	catch(Exception e){
	  		e.printStackTrace();
	  	}
	  	finally{
	  		indexEnd();
	  		prepareData.endDatebase();
	  	}
	}

	@Override
    public void indexEnd(){//数据库+lcene关闭 
    	createIndex.indexEnd();
    }
	
    @Override
  	public void deleteAll(){
    	indexInit();
      	createIndex.deleteAll();
      	createIndex.forceMerge();
      	indexEnd();
  	}
    
  	@Override
	public void deleteIndexRecord() {
  		clearFolderSimhashRecord(keyTemplate);
	}
}
