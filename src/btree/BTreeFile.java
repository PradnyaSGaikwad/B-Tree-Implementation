package btree;

import java.io.*;

import diskmgr.*;
import bufmgr.*;
import global.*;

/**
 * btfile.java This is the main definition of class BTreeFile, which derives
 * from abstract base class IndexFile. It provides an insert/delete interface.
 */
public class BTreeFile extends IndexFile implements GlobalConst {

	private final static int MAGIC0 = 1989;
	
	final int MAX_INDEX_PAGE_CAPACITY = 82;

	final int MAX_LEAF_PAGE_CAPACITY = 62;
	
	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	/**
	 * It causes a structured trace to be written to a file. This output is used
	 * to drive a visualization tool that shows the inner workings of the b-tree
	 * during its operations.
	 *
	 * @param filename
	 *            input parameter. The trace file name
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void traceFilename(String filename) throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}

	/**
	 * Stop tracing. And close trace file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

	private BTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	/**
	 * Access method to data member.
	 * 
	 * @return Return a BTreeHeaderPage object that is the header page of this
	 *         btree file.
	 */
	public BTreeHeaderPage getHeaderPage() {
		return headerPage;
	}

	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno)
			throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}

	private void delete_file_entry(String filename)
			throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty)
			throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	/**
	 * BTreeFile class an index file with given filename should already exist;
	 * this opens it.
	 *
	 * @param filename
	 *            the B+ tree file name. Input parameter.
	 * @exception GetFileEntryException
	 *                can not ger the file from DB
	 * @exception PinPageException
	 *                failed when pin a page
	 * @exception ConstructPageException
	 *                BT page constructor failed
	 */
	public BTreeFile(String filename) throws GetFileEntryException,
			PinPageException, ConstructPageException {

		headerPageId = get_file_entry(filename);

		headerPage = new BTreeHeaderPage(headerPageId);
		dbname = new String(filename);
		/*
		 * 
		 * - headerPageId is the PageId of this BTreeFile's header page; -
		 * headerPage, headerPageId valid and pinned - dbname contains a copy of
		 * the name of the database
		 */
	}

	/**
	 * if index file exists, open it; else create it.
	 *
	 * @param filename
	 *            file name. Input parameter.
	 * @param keytype
	 *            the type of key. Input parameter.
	 * @param keysize
	 *            the maximum size of a key. Input parameter.
	 * @param delete_fashion
	 *            full delete or naive delete. Input parameter. It is either
	 *            DeleteFashion.NAIVE_DELETE or DeleteFashion.FULL_DELETE.
	 * @exception GetFileEntryException
	 *                can not get file
	 * @exception ConstructPageException
	 *                page constructor failed
	 * @exception IOException
	 *                error from lower layer
	 * @exception AddFileEntryException
	 *                can not add file into DB
	 */
	public BTreeFile(String filename, int keytype, int keysize,
			int delete_fashion) throws GetFileEntryException,
			ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
			headerPage.setType(NodeType.BTHEAD);
		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}

		dbname = new String(filename);

	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 *
	 * @exception PageUnpinnedException
	 *                error from the lower layer
	 * @exception InvalidFrameNumberException
	 *                error from the lower layer
	 * @exception HashEntryNotFoundException
	 *                error from the lower layer
	 * @exception ReplacerException
	 *                error from the lower layer
	 */
	public void close() throws PageUnpinnedException,
			InvalidFrameNumberException, HashEntryNotFoundException,
			ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	/**
	 * Destroy entire B+ tree file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 * @exception IteratorException
	 *                iterator error
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception FreePageException
	 *                error when free a page
	 * @exception DeleteFileEntryException
	 *                failed when delete a file from DM
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                failed when pin a page
	 */
	public void destroyFile() throws IOException, IteratorException,
			UnpinPageException, FreePageException, DeleteFileEntryException,
			ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException,
			IteratorException, PinPageException, ConstructPageException,
			UnpinPageException, FreePageException {

		BTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new BTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page,
					headerPage.get_keyType());
			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage
					.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // BTLeafPage

			unpinPage(pageno);
			freePage(pageno);
		}

	}

	private void updateHeader(PageId newRoot) throws IOException,
			PinPageException, UnpinPageException {

		BTreeHeaderPage header;
		
		header = new BTreeHeaderPage(pinPage(headerPageId));

		header.set_rootId(newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */);

		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	/**
	 * insert record with the given key and rid
	 *
	 * @param key
	 *            the key of the record. Input parameter.
	 * @param rid
	 *            the rid of the record. Input parameter.
	 * @exception KeyTooLongException
	 *                key size exceeds the max keysize.
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IOException
	 *                error from the lower layer
	 * @exception LeafInsertRecException
	 *                insert error in leaf page
	 * @exception IndexInsertRecException
	 *                insert error in index page
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception NodeNotMatchException
	 *                node not match index page nor leaf page
	 * @exception ConvertException
	 *                error when convert between revord and byte array
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error when search
	 * @exception IteratorException
	 *                iterator error
	 * @exception LeafDeleteException
	 *                error when delete in leaf page
	 * @exception InsertException
	 *                error when insert in index page
	 */
	public void insert(KeyClass key, RID rid) throws KeyTooLongException,
			KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException,
			UnpinPageException, PinPageException, NodeNotMatchException,
			ConvertException, DeleteRecException, IndexSearchException,
			IteratorException, LeafDeleteException, InsertException,
			IOException

	{
		//check if the tree is empty, if empty create a leafPage and insert first record
		if(headerPage.get_rootId().pid == INVALID_PAGE) {
			//create first leafPage
			BTLeafPage newRootPage = new BTLeafPage(headerPage.get_maxKeySize());			
			PageId newRootPageId = newRootPage.getCurPage();
			
			pinPage(newRootPageId);
			
			//Set next and previous page pointer on newRootPage that is leafPage
			newRootPage.setNextPage(new PageId(INVALID_PAGE));
			newRootPage.setPrevPage(new PageId(INVALID_PAGE));
			
			newRootPage.insertRecord(key, rid);			
			unpinPage(newRootPageId, true); //unpin newRootPage, do we have to do it? why? We have not pinned it.
			updateHeader(newRootPageId);
			
			return ;			
		}
		// if tree is not empty, insert the record
		KeyDataEntry newRootEntry = null;		
		newRootEntry = _insert(key, rid, headerPage.get_rootId());
		
		if(newRootEntry == null) {		
			//Record inserted without a need for split
			return ;			
		} else {				
			//split occurred !!! Create a new IndexPage and updateHeader
			BTIndexPage newIndexPage = new BTIndexPage(headerPage.get_keyType());
			PageId newIndexPageId = newIndexPage.getCurPage();
			
			pinPage(newIndexPageId);
			
			newIndexPage.insertKey(newRootEntry.key, 
					((IndexData)newRootEntry.data).getData());
			
			newIndexPage.setPrevPage(headerPage.get_rootId());
			
			unpinPage(newIndexPageId, true /*dirty*/);
			updateHeader(newIndexPageId);		
		}
		return ;
		
	}

	private KeyDataEntry _insert(KeyClass key, RID rid, PageId currentPageId)
			throws PinPageException, IOException, ConstructPageException,
			LeafDeleteException, ConstructPageException, DeleteRecException,
			IndexSearchException, UnpinPageException, LeafInsertRecException,
			ConvertException, IteratorException, IndexInsertRecException,
			KeyNotMatchException, NodeNotMatchException, InsertException

	{
		Page page;
		BTSortedPage sortedPage;
		KeyDataEntry upEntry = new KeyDataEntry(key, rid);
		page = pinPage(currentPageId); //Create a new empty page and pin it
		sortedPage = new BTSortedPage(page, currentPageId.pid); //associate the SortedPage instance with Page instance
		
		if(sortedPage.getType() == NodeType.INDEX) {
			//associate BTIndexPage instance with the sortedPage instance
			BTIndexPage currentIndexPage = new BTIndexPage(sortedPage, headerPage.get_keyType());
			PageId currentIndexPageId = currentIndexPage.getCurPage();		
			PageId nextPageId = currentIndexPage.getPageNoByKey(key);
	
			unpinPage(currentIndexPageId); //Dirty = false
			
			upEntry = _insert(key, rid, nextPageId);
								
			pinPage(currentIndexPageId);
			
			if(upEntry == null) {
				//No split occurred!
				return null;
			}
			
			//Split occurred
			PageId upEntryPageId = ((IndexData) upEntry.data).getData();
						
			//If currentIndexPage has space for new entry, insert it
			if(currentIndexPage.available_space() >= BT.getKeyDataLength(
					upEntry.key, NodeType.INDEX)) {
				currentIndexPage.insertKey(upEntry.key, upEntryPageId);
				unpinPage(currentIndexPageId, true);
				return null;
			} else {
				//split the index node
				BTIndexPage newIndexPage = new BTIndexPage(currentIndexPage.keyType);
				PageId newIndexPageId = newIndexPage.getCurPage();
				pinPage(newIndexPageId);
				
				KeyDataEntry tempData;
				RID delRid = new RID();	
				
				//Transfer all currentIndexPage entries to newIndexPage
				for (tempData=currentIndexPage.getFirst(delRid);
						tempData!=null;
						tempData = currentIndexPage.getFirst(delRid)) {
						
					newIndexPage.insertKey(tempData.key, ((IndexData) tempData.data).getData());
					try {
						currentIndexPage.deleteKey(tempData.key);
					} catch (IndexFullDeleteException e) {
						System.out.println("Delete key :" +tempData.key+ "for indexPage : "
								+ ""+currentIndexPageId+ " failed during index split "
										+ "and redistribution of elements");
						e.printStackTrace();
					}
				}
				//Transfer back entries to currentIndexPage until both pages are equally filled
				for (tempData=newIndexPage.getFirst(delRid);
						tempData!=null;
						tempData = newIndexPage.getFirst(delRid)) {
						
					if(currentIndexPage.numberOfRecords() <  newIndexPage.numberOfRecords()) {
						currentIndexPage.insertKey(tempData.key, ((IndexData) tempData.data).getData());
						try {
							newIndexPage.deleteKey(tempData.key);
						} catch (IndexFullDeleteException e) {
							System.out.println("Delete key :" +tempData.key+ "for indexPage : "
									+ ""+newIndexPage+ " failed during index split and"
											+ " redistribution of node elements");
							e.printStackTrace();
						}
					} else {
						try {
							newIndexPage.deleteKey(tempData.key);
						} catch (IndexFullDeleteException e) {
							System.out.println("Delete key :" +tempData.key+ "for indexPage : "
									+ ""+currentIndexPage+ " failed during index split and"
											+ " redistribution of node elements");
							e.printStackTrace();
						}
						break;
					}
				}
				
				//Compare keys to know which page to insert the record to
				if(BT.keyCompare(upEntry.key, tempData.key) > 0) {
					try {
						newIndexPage.insertRecord(upEntry);
					} catch (InsertRecException e) {
						System.out.println("Insert of new key " + upEntryPageId + " in index page"
								+ " " + newIndexPageId+ " has failed.");
						e.printStackTrace();
					}
				} else {
					try {
						currentIndexPage.insertRecord(upEntry);
					} catch (InsertRecException e) {
						System.out.println("Insert of new key " + upEntryPageId + " in index page"
								+ " " + currentIndexPage.getCurPage()+ " has failed.");
						e.printStackTrace();
					}
				}
				
				
				//Get the first key of new index node to push it to the parent node
				upEntry=tempData;
				
				PageId leftLinkPageIdOfCurrentIndexPage = ((IndexData) tempData.data).getData();
				
				//set prev of new index node
				newIndexPage.setLeftLink(leftLinkPageIdOfCurrentIndexPage);
				
				unpinPage(currentIndexPageId, true);
				
				//Get the first key of new index node to push it to the parent node
				//upEntry=newIndexPage.getFirst(delRid);
		
				IntegerKey intkey = (IntegerKey)upEntry.key;
				KeyDataEntry indexUpEntry=new KeyDataEntry(intkey.getKey(), newIndexPage.getCurPage());
				
				//Delete first record from index node
				unpinPage(newIndexPageId, true /* dirty */);
				
				return indexUpEntry;
			}			
			
		} else if(sortedPage.getType() == NodeType.LEAF) {
			//associate BTLeafPage instance with the sortedPage instance
		    BTLeafPage currentLeafPage = new BTLeafPage(sortedPage,
				headerPage.get_keyType());
			PageId currentLeafPageId = currentLeafPage.getCurPage();
			
			//If currentLeafPage has space for new record, insert record
			if(currentLeafPage.available_space() >=
					BT.getKeyDataLength(upEntry.key, NodeType.LEAF)) {
				
				try {
					currentLeafPage.insertRecord(upEntry);
				} catch (InsertRecException e) {
					System.out.println("Insert of record " + upEntry.key + " in leaf page"
							+ " " + currentLeafPage.getCurPage()+ " has failed.");
					e.printStackTrace();
				}
				unpinPage(currentLeafPageId, true);
				return null;
				
			} else {
				//If leafPage is full, split the page
				BTLeafPage newLeafPage = new BTLeafPage(headerPage.get_keyType());
				PageId newLeafPageId = newLeafPage.getCurPage();
				
				pinPage(newLeafPageId);
				
				KeyDataEntry tempData;
				RID delRid = new RID();
				//Transfer all currentLeafPage record to newLeafPage
				for(tempData = currentLeafPage.getFirst(delRid);
						tempData != null; 
						tempData = currentLeafPage.getFirst(delRid)) {
					try {
						newLeafPage.insertRecord(tempData);
					} catch (InsertRecException e) {
						System.out.println("Insert of record " + tempData.key + " in leaf page"
								+ " " + newLeafPage.getCurPage()+ " has failed during "
										+ "leaf split and redistribution of records.");
						e.printStackTrace();
					}
					currentLeafPage.deleteSortedRecord(delRid);
				}
				//Transfer back records to currentLeafPage until both pages are equally filled
				for(tempData = newLeafPage.getFirst(delRid);
						tempData != null; 
						tempData = newLeafPage.getFirst(delRid)) {
					if(currentLeafPage.available_space() > newLeafPage.available_space()) {
						try {
							currentLeafPage.insertRecord(tempData);
						} catch (InsertRecException e) {
							System.out.println("Insert of record " + tempData.key + " in leaf page"
									+ " " + currentLeafPage.getCurPage()+ " has failed during "
											+ "leaf split and redistribution of records.");
							e.printStackTrace();
						}
						newLeafPage.deleteSortedRecord(delRid);
					} else {
						break;
					}
				}
				
				//set previous pointer of nextOfCurrent page					
				if(currentLeafPage.getNextPage().pid != INVALID_PAGE) {
					PageId nextPointerOfCurrentPageId = currentLeafPage.getNextPage();
					BTSortedPage nextOfCurrentPage = new BTSortedPage(
							pinPage(nextPointerOfCurrentPageId), nextPointerOfCurrentPageId.pid);
					
					nextOfCurrentPage.setPrevPage(newLeafPageId);
					unpinPage(nextPointerOfCurrentPageId, true);
				}
				//set previous and next pointers of newLeafPage, 
				newLeafPage.setNextPage(currentLeafPage.getNextPage());
				newLeafPage.setPrevPage(currentLeafPageId);
				//set next pointer of currentLeafPage
				currentLeafPage.setNextPage(newLeafPageId);
				
				//Insert record in correct leaf node by using method for key compare
				if(BT.keyCompare(upEntry.key, tempData.key) > 0) {
					try {
						newLeafPage.insertRecord(upEntry);
					} catch (InsertRecException e) {
						System.out.println("Insert of new record " + upEntry.key + " in leaf page"
								+ " " + newLeafPage.getCurPage()+ " has failed.");
						e.printStackTrace();
					}
				} else {
					try {
						currentLeafPage.insertRecord(upEntry);
					} catch (InsertRecException e) {
						System.out.println("Insert of new record " + upEntry.key + " in leaf page"
								+ " " + currentLeafPage.getCurPage()+ " has failed.");
						e.printStackTrace();
					}
				}
				
				unpinPage(currentLeafPageId, true /* dirty */);
				
				//Get first record of leaf node which will be used to copy key to parent index node
				tempData = newLeafPage.getFirst(rid);
		
				IntegerKey intkey = (IntegerKey) tempData.key;
				KeyDataEntry leafUpEntry=new KeyDataEntry(intkey.getKey(), newLeafPageId);
				
				unpinPage(newLeafPageId, true /* dirty */);
				
				return leafUpEntry;
			}
		} else {
			throw new InsertException("Insert function failed to insert a record");
		}
	}

	private KeyClass IntegerKey(int i) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * delete leaf entry given its <key, rid> pair. `rid' is IN the data entry;
	 * it is not the id of the data entry)
	 *
	 * @param key
	 *            the key in pair <key, rid>. Input Parameter.
	 * @param rid
	 *            the rid in pair <key, rid>. Input Parameter.
	 * @return true if deleted. false if no such record.
	 * @exception DeleteFashionException
	 *                neither full delete nor naive delete
	 * @exception LeafRedistributeException
	 *                redistribution error in leaf pages
	 * @exception RedistributeException
	 *                redistribution error in index pages
	 * @exception InsertRecException
	 *                error when insert in index page
	 * @exception KeyNotMatchException
	 *                key is neither integer key nor string key
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception IndexInsertRecException
	 *                error when insert in index page
	 * @exception FreePageException
	 *                error in BT page constructor
	 * @exception RecordNotFoundException
	 *                error delete a record in a BT page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception IndexFullDeleteException
	 *                fill delete error
	 * @exception LeafDeleteException
	 *                delete error in leaf page
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error in search in index pages
	 * @exception IOException
	 *                error from the lower layer
	 *
	 */
	public boolean Delete(KeyClass key, RID rid) throws DeleteFashionException,
			LeafRedistributeException, RedistributeException,
			InsertRecException, KeyNotMatchException, UnpinPageException,
			IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException,
			IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException,
			IOException  {
		
			//check if the tree is empty, if empty return false
			if(headerPage.get_rootId().pid == INVALID_PAGE) {				
				return false;			
			}
			KeyDataEntry keyDataEntry = null;
			try {
				keyDataEntry = fullDelete(key, rid, headerPage.get_rootId(), null);
			} catch (NodeNotMatchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ConvertException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(keyDataEntry == null) {
				return true;
			} 
		return false;
		
	}
	
    PageId getLeftSibling(KeyClass key, PageId pageNo, BTIndexPage indexPage)
		        throws IndexFullDeleteException, IOException, KeyNotMatchException, 
		        NodeNotMatchException, ConvertException {
		 
		 int indexPageSlotCount = indexPage.getSlotCnt();
         if(indexPageSlotCount == 0)
            return null;		        
		 int i = indexPageSlotCount - 1;
		 while(i>=0) {
			 
			 KeyDataEntry entry = BT.getEntryFromBytes(indexPage.getpage(), 
		        		indexPage.getSlotOffset(i), indexPage.getSlotLength(i), headerPage.get_keyType(), (short)11);
			 if(BT.keyCompare(key, entry.key) < 0) {
				 i--;
		         continue;
			 }
			 /* Loop/switch isn't completed */
		        if(i == 0) {
			        try
			        {
			            pageNo = indexPage.getLeftLink();
			            return pageNo;
			        }
			        catch(Exception e)
			        {
			            throw new IndexFullDeleteException(e, "Get sibling failed");
			        }
		        } else {
		            entry = BT.getEntryFromBytes(indexPage.getpage(), 
			        		indexPage.getSlotOffset(i - 1), indexPage.getSlotLength(i - 1), headerPage.get_keyType(), (short)11);
			        pageNo = ((IndexData)entry.data).getData();
			        return pageNo;
		        }
		        	
		 }
		 return null;	        
   }
	 
    PageId getRightSibling(KeyClass key, PageId pageNo, BTIndexPage indexPage)
	        throws IndexFullDeleteException, IOException, KeyNotMatchException, 
	        NodeNotMatchException, ConvertException {
	 
		 int indexPageSlotCount = indexPage.getSlotCnt();
	     if(indexPageSlotCount == 0)
	        return null;		        
		 int i = indexPageSlotCount - 1;
		 while(i>=0) {
			 
			 KeyDataEntry entry = BT.getEntryFromBytes(indexPage.getpage(), 
		        		indexPage.getSlotOffset(i), indexPage.getSlotLength(i), headerPage.get_keyType(), (short)11);
			 if(BT.keyCompare(key, entry.key) < 0) {
				 i--;
		         continue;
			 }
			 if(i == indexPageSlotCount -1) {
				 return null;
			 }
			 /* Loop/switch isn't completed */
	        entry = BT.getEntryFromBytes(indexPage.getpage(), 
	        		indexPage.getSlotOffset(i + 1), indexPage.getSlotLength(i + 1), headerPage.get_keyType(), (short)11);
	        pageNo = ((IndexData)entry.data).getData();
	        return pageNo;
		 }
		 if (i < 0) {
			 KeyDataEntry entry = BT.getEntryFromBytes(indexPage.getpage(), 
		        		indexPage.getSlotOffset(i + 1), indexPage.getSlotLength(i + 1), headerPage.get_keyType(), (short)11);
	        pageNo = ((IndexData)entry.data).getData();
	        return pageNo;
		 }
		return null;	       	        
   }

	private KeyDataEntry fullDelete(KeyClass key, RID rid, PageId currentPageId, BTIndexPage parentPage) 
			throws LeafDeleteException, KeyNotMatchException, PinPageException,
			ConstructPageException, IOException, UnpinPageException,
			PinPageException, IndexSearchException, IteratorException, 
			LeafRedistributeException, DeleteRecException, NodeNotMatchException, 
			IndexFullDeleteException, RedistributeException, ConvertException {
		
		Page page;
		BTSortedPage sortedPage;
		
		KeyDataEntry upDelete = new KeyDataEntry(key, rid);
		page = pinPage(currentPageId); //Create a new empty page and pin it
		sortedPage = new BTSortedPage(page, currentPageId.pid); //associate the SortedPage instance with Page instance
		
		
		if(sortedPage.getType() == NodeType.INDEX) { // check if the page of Index type
			
			//associate BTIndexPage instance with the sortedPage instance
			BTIndexPage currentIndexPage = new BTIndexPage(sortedPage, headerPage.get_keyType());
			PageId currentIndexPageId = currentIndexPage.getCurPage();	
			PageId nextPageId = currentIndexPage.getPageNoByKey(key);
			
			unpinPage(currentIndexPageId); //Dirty = false
			
			upDelete = fullDelete(key, rid, nextPageId, currentIndexPage);
			pinPage(currentIndexPageId);
			
			if(upDelete == null) {
				//No merge occurred!
				return null;
			}
			
			//Merge occurred
			RID tempRid = new RID();
			KeyDataEntry tempData = null;
			PageId upDeletePageId = ((IndexData) upDelete.data).getData();
			
			
			KeyDataEntry firstElementOfCurrentIndexPage = currentIndexPage.getFirst(tempRid);//first element of deleted-element page
			//currentIndexPage.deleteSortedRecord(new RID(upDeletePageId, upDelete.key));
			
			int indexPageSlotCount = currentIndexPage.getSlotCnt();
		    int i = indexPageSlotCount - 1;
			while(i>=0) {
				 
				KeyDataEntry entry = BT.getEntryFromBytes(currentIndexPage.getpage(), 
						currentIndexPage.getSlotOffset(i), currentIndexPage.getSlotLength(i), headerPage.get_keyType(), (short)11);
				if(BT.keyCompare(upDelete.key, entry.key) >= 0) {
					currentIndexPage.deleteKey(upDelete.key);
					break;
				}
				i--;
			}
				
			//If currentLeafPage has available space <= 50%
			if(currentIndexPage.getSlotCnt() >= MAX_INDEX_PAGE_CAPACITY/2) {	
				return null;
			} 
			if(parentPage == null) { // check if header is pointing to the current index page
				if(sortedPage.getType() == NodeType.INDEX) {	
					
					if(currentIndexPage.empty()) { // If current Index page where header is pointing is empty
						PageId rearrangePageid = currentIndexPage.getLeftLink();
						
						Page pageTemp = pinPage(rearrangePageid); //Create a new empty page and pin it
						BTSortedPage sortedPageTemp = new BTSortedPage(pageTemp, rearrangePageid.pid); //associate the SortedPage instance with Page instance
						BTIndexPage rearrangeIndexPage = null;
						BTLeafPage rearrangeLeafPage = null;
						
						currentIndexPage.setPrevPage(new PageId(INVALID_PAGE));
						currentIndexPage.setNextPage(new PageId(INVALID_PAGE));
						currentIndexPage = null;
						
						if(sortedPageTemp.getType() == NodeType.INDEX) { 
							rearrangeIndexPage = new BTIndexPage(sortedPageTemp, headerPage.get_keyType());
							rearrangePageid = rearrangeIndexPage.getCurPage();	
							updateHeader(rearrangePageid);	
						} 
						if(sortedPageTemp.getType() == NodeType.LEAF) {
							rearrangeLeafPage = new BTLeafPage(sortedPageTemp, headerPage.get_keyType());
							rearrangePageid = rearrangeLeafPage.getCurPage();	
							updateHeader(rearrangePageid);
							if(rearrangeLeafPage.empty() || rearrangeLeafPage.getSlotCnt() == 0) {
								updateHeader(new PageId(INVALID_PAGE));
								rearrangeLeafPage = null;
							}
						}						
					}
						
					return null;
				}
			}

			if(parentPage != null) { // If this is just a Index page with a parent index
				//If leafPage available space is > 50%
				//Check if siblings have available space < 50%
				
				PageId parentPageId = parentPage.getCurPage();
				page = pinPage(parentPageId); //Create a new empty page and pin it
				sortedPage = new BTSortedPage(page, parentPageId.pid); //associate the SortedPage instance with Page instance
				BTIndexPage currentParentPage = new BTIndexPage(sortedPage, headerPage.get_keyType());
				PageId currentParentPageId = currentParentPage.getCurPage();
				
				PageId leftIndexPageId = getLeftSibling(upDelete.key, currentParentPageId, currentParentPage);
				if(leftIndexPageId != null) { // check if left sibling exists
					page = pinPage(leftIndexPageId); //Create a new empty page and pin it
					sortedPage = new BTSortedPage(page, leftIndexPageId.pid); //associate the SortedPage instance with Page instance
					BTIndexPage leftSiblingOfCurrentIndexPage = new BTIndexPage(sortedPage, headerPage.get_keyType());
					PageId leftSiblingOfCurrentIndexPageId = leftSiblingOfCurrentIndexPage.getCurPage();
					
					PageId leftLinkPageIdOfcurrentIndexPage = currentIndexPage.getLeftLink();
					KeyDataEntry firstElementOFLeftLinkPageOfCurrentIndex = null;
					
					if(leftLinkPageIdOfcurrentIndexPage.pid != INVALID_PAGE) { // Get the left link page of current index page
						Page page1 = pinPage(leftLinkPageIdOfcurrentIndexPage); //Create a new empty page and pin it
						BTSortedPage sortedPage1 = new BTSortedPage(page1, leftLinkPageIdOfcurrentIndexPage.pid); //associate the SortedPage instance with Page instance
						BTLeafPage leftLinkPageOfcurrentIndexPage = new BTLeafPage(sortedPage1, headerPage.get_keyType());
						leftLinkPageIdOfcurrentIndexPage = leftLinkPageOfcurrentIndexPage.getCurPage();
						
						firstElementOFLeftLinkPageOfCurrentIndex = leftLinkPageOfcurrentIndexPage.getFirst(tempRid);
						
					}			
				
					// If left sibling is more than 50% full, redistribute
					if(leftSiblingOfCurrentIndexPage.getSlotCnt() > MAX_INDEX_PAGE_CAPACITY/2) {
						
						// First transfer the left link entry of current page to the left sibling page
						if(firstElementOFLeftLinkPageOfCurrentIndex != null) {
							try {
								leftSiblingOfCurrentIndexPage.insertKey(
										firstElementOFLeftLinkPageOfCurrentIndex.key, leftLinkPageIdOfcurrentIndexPage);
								} catch (IndexInsertRecException e1) {
									System.out.println("Insert of left Link record from right sibling Index Page to left Index page"
											+ " " + currentIndexPage.getCurPage()+ " has failed during "
													+ "Index merge.");
									e1.printStackTrace();
								}
						}
	     				// Transfer all entries from current index page to left Index, until left sibling index is full
						for(tempData = currentIndexPage.getFirst(tempRid);
								tempData != null; 
								tempData = currentIndexPage.getFirst(tempRid)) {
								if(leftSiblingOfCurrentIndexPage.available_space() >= BT.getKeyDataLength(tempData.key, NodeType.INDEX)) {
									try {
										leftSiblingOfCurrentIndexPage.insertKey(tempData.key, 
												((IndexData) tempData.data).getData());
									} catch (IndexInsertRecException e) {
										System.out.println("Insert of record " + tempData.key + " in left Index page"
												+ " " + leftSiblingOfCurrentIndexPage.getCurPage()+ " has failed during "
														+ "index redistribution.");
										e.printStackTrace();
									}
									currentIndexPage.deleteSortedRecord(tempRid);
								} else {
									break;
								}
							}
							
							int restElementsInCurrentNode = currentIndexPage.numberOfRecords();
							int elementsInLeftNode = leftSiblingOfCurrentIndexPage.numberOfRecords();
							int tempMedian = (int) Math.floor((restElementsInCurrentNode + elementsInLeftNode) / 2);
							
							BTIndexPage newTempIndexPage = new BTIndexPage(headerPage.get_keyType());
							PageId newTempIndexPageId = newTempIndexPage.getCurPage();
							
							pinPage(newTempIndexPageId);
							
							// Transfer entries that are half of (current Index entries + left sibling index entries) to a temporary index page
							for(tempData = leftSiblingOfCurrentIndexPage.getFirst(tempRid);
								tempData != null; 
								tempData = leftSiblingOfCurrentIndexPage.getFirst(tempRid)) {
								if(tempMedian>0) {
									try {
										newTempIndexPage.insertKey(tempData.key, 
												((IndexData) tempData.data).getData());
									} catch (IndexInsertRecException e) {
										System.out.println("Insert of record " + tempData.key + " in temporary Index page"
												+ " " + newTempIndexPage.getCurPage()+ " has failed during "
														+ "Index redistribution.");
										e.printStackTrace();
									}
									leftSiblingOfCurrentIndexPage.deleteSortedRecord(tempRid);
									tempMedian--;
								} else {
									break;
								}
							}
							// Transfer remaining entries from left sibling index page to current Index
							for(tempData = leftSiblingOfCurrentIndexPage.getFirst(tempRid);
									tempData != null; 
									tempData = leftSiblingOfCurrentIndexPage.getFirst(tempRid)) {
									try {
										currentIndexPage.insertKey(tempData.key, 
												((IndexData) tempData.data).getData());
									} catch (IndexInsertRecException e) {
										System.out.println("Insert of record " + tempData.key + " in current Index page"
												+ " " + currentIndexPage.getCurPage()+ " has failed during "
														+ "Index redistribution.");
										e.printStackTrace();
									}
									leftSiblingOfCurrentIndexPage.deleteSortedRecord(tempRid);
									
								}
							// Transfer all entries from temporary index page to left sibling Index
	     					for(tempData = newTempIndexPage.getFirst(tempRid);
								tempData != null; 
								tempData = newTempIndexPage.getFirst(tempRid)) {
								try {
									leftSiblingOfCurrentIndexPage.insertKey(tempData.key, 
											((IndexData) tempData.data).getData());
								} catch (IndexInsertRecException e) {
									System.out.println("Insert of record " + tempData.key + " in left Index page"
											+ " " + leftSiblingOfCurrentIndexPage.getCurPage()+ " has failed during "
													+ "Index redistribution.");
									e.printStackTrace();
								}
								newTempIndexPage.deleteSortedRecord(tempRid);
							}
							
     						// set the Left link of current index approriately 
     						currentIndexPage.setLeftLink(((IndexData)currentIndexPage.getFirst(tempRid).data).getData());
     						currentIndexPage.deleteKey(currentIndexPage.getFirst(tempRid).key);
     						
     						PageId newLeftLinkPageIdOfCurrentIndex = currentIndexPage.getLeftLink();
    						KeyDataEntry firstElementOFNewLeftLinkPageOfCurrentIndex = null;
    						// Get the key entry to be updated in the parent index
    						if(newLeftLinkPageIdOfCurrentIndex.pid != INVALID_PAGE) {
    							Page page1 = pinPage(newLeftLinkPageIdOfCurrentIndex); //Create a new empty page and pin it
    							BTSortedPage sortedPage1 = new BTSortedPage(page1, newLeftLinkPageIdOfCurrentIndex.pid); //associate the SortedPage instance with Page instance
    							BTLeafPage newLeftLinkPageOfCurrentIndex = new BTLeafPage(sortedPage1, headerPage.get_keyType());
    							newLeftLinkPageIdOfCurrentIndex = newLeftLinkPageOfCurrentIndex.getCurPage();
    							
    							firstElementOFNewLeftLinkPageOfCurrentIndex = newLeftLinkPageOfCurrentIndex.getFirst(tempRid);		
    						}		
    						
    						//adjust key in the parent index
    						parentPage.adjustKey(firstElementOFNewLeftLinkPageOfCurrentIndex.key, firstElementOfCurrentIndexPage.key);
    													
							return null;
							//end of redistribution from left sibling - index node
					}
				}
				
				PageId rightIndexPageId = getRightSibling(upDelete.key, currentParentPageId, currentParentPage);
				
				//beginning of redistribution from right sibling - index node
				
				if(rightIndexPageId != null) {
					page = pinPage(rightIndexPageId); //Create a new empty page and pin it
					sortedPage = new BTSortedPage(page, rightIndexPageId.pid); //associate the SortedPage instance with Page instance
					BTIndexPage rightSiblingOfCurrentIndexPage = new BTIndexPage(sortedPage, headerPage.get_keyType());
					PageId rightSiblingOfCurrentIndexPageId = rightSiblingOfCurrentIndexPage.getCurPage();
					
					PageId leftLinkPageIdOfRightSibling = rightSiblingOfCurrentIndexPage.getLeftLink();
					KeyDataEntry firstElementOFLeftLinkPageOfRightSibling = null;
					
					// Get the leftlink of the right sibling index page
					if(leftLinkPageIdOfRightSibling.pid != INVALID_PAGE) {
						Page page1 = pinPage(leftLinkPageIdOfRightSibling); //Create a new empty page and pin it
						BTSortedPage sortedPage1 = new BTSortedPage(page1, leftLinkPageIdOfRightSibling.pid); //associate the SortedPage instance with Page instance
						BTLeafPage leftLinkPageOfRightSibling = new BTLeafPage(sortedPage1, headerPage.get_keyType());
						leftLinkPageIdOfRightSibling = leftLinkPageOfRightSibling.getCurPage();
						
						firstElementOFLeftLinkPageOfRightSibling = leftLinkPageOfRightSibling.getFirst(tempRid);
						
					}						
						
					// If right sibling index page is more than 50% full, redistribute	
					if(rightSiblingOfCurrentIndexPage.getSlotCnt() > MAX_INDEX_PAGE_CAPACITY/2) {
						
						//Then redistribute
						tempData = null;
						KeyDataEntry firstElementOfRightIndexPage =  rightSiblingOfCurrentIndexPage.getFirst(tempRid);//first element of deleted-element page
						
						// Transfer records from right sibling index page to current index page, until both have equal records
						for(tempData = rightSiblingOfCurrentIndexPage.getFirst(tempRid);
							tempData != null; 
							tempData = rightSiblingOfCurrentIndexPage.getFirst(tempRid)) {
							if(currentIndexPage.available_space() >= rightSiblingOfCurrentIndexPage.available_space()) {
								try {
									currentIndexPage.insertKey(tempData.key, ((IndexData) tempData.data).getData());
									
								} catch (IndexInsertRecException e) {
									System.out.println("Insert of record " + tempData.key + " in current Index page"
											+ " " + currentIndexPage.getCurPage()+ " has failed during "
													+ "Index redistribution.");
									e.printStackTrace();
								}
								rightSiblingOfCurrentIndexPage.deleteSortedRecord(tempRid);
							} else {
								break;
							}
						}	
					
						// insert also the left link of the right index page into the current index page
						if(firstElementOFLeftLinkPageOfRightSibling != null) {
							try {
								currentIndexPage.insertKey(
										firstElementOFLeftLinkPageOfRightSibling.key, leftLinkPageIdOfRightSibling);
								} catch (IndexInsertRecException e1) {
									System.out.println("Insert of left Link record from right sibling Index Page to left Index page"
											+ " " + currentIndexPage.getCurPage()+ " has failed during "
													+ "Index merge.");
									e1.printStackTrace();
								}
						}
						
						// reset the left link of right index page correctly
						rightSiblingOfCurrentIndexPage.setLeftLink(((IndexData)rightSiblingOfCurrentIndexPage.getFirst(tempRid).data).getData());
						rightSiblingOfCurrentIndexPage.deleteKey(rightSiblingOfCurrentIndexPage.getFirst(tempRid).key);
						
						// Get the first element of the left link page of the right slibling index page to adjust key in parent index
						PageId newLeftLinkPageIdOfRightSibling = rightSiblingOfCurrentIndexPage.getLeftLink();
						KeyDataEntry firstElementOFNewLeftLinkPageOfRightSibling = null;
						
						if(newLeftLinkPageIdOfRightSibling.pid != INVALID_PAGE) {
							Page page1 = pinPage(newLeftLinkPageIdOfRightSibling); //Create a new empty page and pin it
							BTSortedPage sortedPage1 = new BTSortedPage(page1, newLeftLinkPageIdOfRightSibling.pid); //associate the SortedPage instance with Page instance
							BTLeafPage newLeftLinkPageOfRightSibling = new BTLeafPage(sortedPage1, headerPage.get_keyType());
							newLeftLinkPageIdOfRightSibling = newLeftLinkPageOfRightSibling.getCurPage();
							
							firstElementOFNewLeftLinkPageOfRightSibling = newLeftLinkPageOfRightSibling.getFirst(tempRid);
							
						}		
						
						//Update key in parent index
						parentPage.adjustKey(firstElementOFNewLeftLinkPageOfRightSibling.key, firstElementOfRightIndexPage.key);
										
						return null; // End of redistribution of right sibling index page
					}					
				}
				// Code for merge begins here
				if(leftIndexPageId != null) { // Check if there is left index page
					page = pinPage(leftIndexPageId); //Create a new empty page and pin it
					sortedPage = new BTSortedPage(page, leftIndexPageId.pid); //associate the SortedPage instance with Page instance
					BTIndexPage leftSiblingOfCurrentIndexPage = new BTIndexPage(sortedPage, headerPage.get_keyType());
					PageId leftSiblingOfCurrentIndexPageId = leftSiblingOfCurrentIndexPage.getCurPage();
					
					PageId leftLinkPageIdOfCurrentPage = currentIndexPage.getLeftLink();
					
					KeyDataEntry firstElementOFLeftLinkPage = null;
					// get left link of the current index page
					if(leftLinkPageIdOfCurrentPage.pid != INVALID_PAGE) {
						Page page1 = pinPage(leftLinkPageIdOfCurrentPage); //Create a new empty page and pin it
						BTSortedPage sortedPage1 = new BTSortedPage(page1, leftLinkPageIdOfCurrentPage.pid); //associate the SortedPage instance with Page instance
						BTLeafPage lefttLinkPageOfCurrentPage = new BTLeafPage(sortedPage1, headerPage.get_keyType());
						leftLinkPageIdOfCurrentPage = lefttLinkPageOfCurrentPage.getCurPage();
						
						firstElementOFLeftLinkPage = lefttLinkPageOfCurrentPage.getFirst(tempRid);
						
					}
					// set left link of the current index page to invalid
					currentIndexPage.setLeftLink(new PageId(INVALID_PAGE));
					
					// if leftSibling is 50% full then merge
					if(leftSiblingOfCurrentIndexPage.getSlotCnt() == MAX_INDEX_PAGE_CAPACITY/2) {							
							
						KeyDataEntry upDataToDelete = currentIndexPage.getFirst(tempRid);						
						// Transfer all element from current Index Page to the left sibling of current index page
						for(tempData = currentIndexPage.getFirst(tempRid);
							tempData != null; 
							tempData = currentIndexPage.getFirst(tempRid)) {
							try {
								leftSiblingOfCurrentIndexPage.insertKey(tempData.key,
										((IndexData) tempData.data).getData());
							} catch (IndexInsertRecException e) {
								System.out.println("Insert of record " + tempData.key + " in left Index page"
										+ " " + leftSiblingOfCurrentIndexPage.getCurPage()+ " has failed during "
												+ "Index merge.");
								e.printStackTrace();
							} 
							currentIndexPage.deleteSortedRecord(tempRid);
						}
						
						// Insert also the left link of current index page to the left sibling
						try {
							leftSiblingOfCurrentIndexPage.insertKey(
									firstElementOFLeftLinkPage.key, leftLinkPageIdOfCurrentPage);
							} catch (IndexInsertRecException e1) {
								System.out.println("Insert of left Link record from current Index Page to left Index page"
										+ " " + leftSiblingOfCurrentIndexPage.getCurPage()+ " has failed during "
												+ "Index merge.");
								e1.printStackTrace();
							}
						
						tempData = firstElementOFLeftLinkPage;
						
						//propagate the delete by passing correct value up in the tree
						IntegerKey intkey = (IntegerKey) tempData.key;
						KeyDataEntry indexUpDelete=new KeyDataEntry(intkey.getKey(), currentIndexPageId);
						
						currentIndexPage = null;
						
						return indexUpDelete;						
					}
				}
				
				if(rightIndexPageId != null) { // If right index page exists for the current index page
					
					page = pinPage(rightIndexPageId); //Create a new empty page and pin it
					sortedPage = new BTSortedPage(page, rightIndexPageId.pid); //associate the SortedPage instance with Page instance
					BTIndexPage rightSiblingOfCurrentIndexPage = new BTIndexPage(sortedPage, headerPage.get_keyType());
					PageId rightSiblingOfCurrentIndexPageId = rightSiblingOfCurrentIndexPage.getCurPage();
					
					PageId leftLinkPageIdOfRightSibling = rightSiblingOfCurrentIndexPage.getLeftLink();
					KeyDataEntry firstElementOFLeftLinkPageOfRightSibling = null;
					
					// get left link page of the right sibling
					if(leftLinkPageIdOfRightSibling.pid != INVALID_PAGE) {
						Page page1 = pinPage(leftLinkPageIdOfRightSibling); //Create a new empty page and pin it
						BTSortedPage sortedPage1 = new BTSortedPage(page1, leftLinkPageIdOfRightSibling.pid); //associate the SortedPage instance with Page instance
						BTLeafPage leftLinkPageOfRightSibling = new BTLeafPage(sortedPage1, headerPage.get_keyType());
						leftLinkPageIdOfRightSibling = leftLinkPageOfRightSibling.getCurPage();
						
						firstElementOFLeftLinkPageOfRightSibling = leftLinkPageOfRightSibling.getFirst(tempRid);
						
					}
					//set the left link of the right sibling to invalid
					rightSiblingOfCurrentIndexPage.setLeftLink(new PageId(INVALID_PAGE));
					
					// If right Sibling is  50% full
					if(rightSiblingOfCurrentIndexPage.getSlotCnt() <= MAX_INDEX_PAGE_CAPACITY/2) {							
							
						KeyDataEntry upDataToDelete = currentIndexPage.getFirst(tempRid);
						//Transfer records from right sibling to the current index page
						for(tempData = rightSiblingOfCurrentIndexPage.getFirst(tempRid);
							tempData != null; 
							tempData = rightSiblingOfCurrentIndexPage.getFirst(tempRid)) {
							try {
								currentIndexPage.insertKey(tempData.key,
										((IndexData) tempData.data).getData());
							} catch (IndexInsertRecException e) {
								System.out.println("Insert of record " + tempData.key + " in right index page"
										+ " " + currentIndexPage.getCurPage()+ " has failed during "
												+ "index merge.");
								e.printStackTrace();
							}
							rightSiblingOfCurrentIndexPage.deleteSortedRecord(tempRid);
						}
						// insert also the left link of right index page to current index page
						if(firstElementOFLeftLinkPageOfRightSibling != null) {
							try {
								currentIndexPage.insertKey(
										firstElementOFLeftLinkPageOfRightSibling.key, leftLinkPageIdOfRightSibling);
								} catch (IndexInsertRecException e1) {
									System.out.println("Insert of left Link record from right sibling Index Page to left Index page"
											+ " " + currentIndexPage.getCurPage()+ " has failed during "
													+ "Index merge.");
									e1.printStackTrace();
								}
						}
						tempData = firstElementOFLeftLinkPageOfRightSibling;
						
						//propagate the delete by passing correct value up in the tree
						IntegerKey intkey = (IntegerKey) tempData.key;
						KeyDataEntry indexUpDelete=new KeyDataEntry(intkey.getKey(), currentIndexPageId);
						
						currentIndexPage = null;
						
						return indexUpDelete;	
					}
				}
			}		
			
		} else if(sortedPage.getType() == NodeType.LEAF) { //check if leaf - code for leaf
			//associate BTLeafPage instance with the sortedPage instance
			
		    BTLeafPage currentLeafPage = new BTLeafPage(sortedPage,
				headerPage.get_keyType());			
			PageId currentLeafPageId = currentLeafPage.getCurPage();
			
			if(parentPage == null) { // Check if there is a parent page for current leaf page
				BTLeafPage leafPageAfterDelete = fullDeleteHelper(key, rid);
				if(leafPageAfterDelete == null) {
					throw new IndexFullDeleteException("Deletion from leaf page failed !! ");
				}
				if(leafPageAfterDelete.empty() || leafPageAfterDelete.getSlotCnt() == 0) {
					updateHeader(new PageId(INVALID_PAGE));
				}
				return null;
			}
			
			
			Page tempPage = pinPage(parentPage.getCurPage()); //Create a new empty page and pin it
			BTSortedPage tempSortedPage = new BTSortedPage(tempPage, parentPage.getCurPage().pid); //associate the SortedPage instance with Page instance
			BTIndexPage currentParentPage = new BTIndexPage(tempSortedPage,
					headerPage.get_keyType());			
			PageId currentParentPageId = currentParentPage.getCurPage();
			RID tempRid = new RID();
			KeyDataEntry firstElementOfCurrentLeafPage = currentLeafPage.getFirst(tempRid);//first element of deleted-element page
			
			// Delete the record
			currentLeafPage = fullDeleteHelper(key, rid);
			
			//If currentLeafPage has available space <= 50%
			if(currentLeafPage == null || currentLeafPage.getSlotCnt() >= MAX_LEAF_PAGE_CAPACITY/2) {
				return null;
			} else {
				
				PageId leftLeafPageId = currentLeafPage.getPrevPage();
				PageId rightLeafPageId = currentLeafPage.getNextPage();
				KeyDataEntry firstElementOfRightLeafPage = null;
				KeyDataEntry tempData = null;
				
				BTLeafPage newTempLeafPage = new BTLeafPage(headerPage.get_keyType());
				PageId newTempLeafPageId = newTempLeafPage.getCurPage();
				
				pinPage(newTempLeafPageId);
				
				if(leftLeafPageId.pid != INVALID_PAGE) { // Check if there is a left leaf
					tempPage = pinPage(leftLeafPageId); //Create a new empty page and pin it
					tempSortedPage = new BTSortedPage(tempPage, leftLeafPageId.pid); //associate the SortedPage instance with Page instance
					BTLeafPage leftLeafPage = new BTLeafPage(tempSortedPage, headerPage.get_keyType());
					leftLeafPageId = leftLeafPage.getCurPage();
					
					int leftMostPageOfParent = currentParentPage.getLeftLink().pid;
					int leftLeafPageIdInt = leftLeafPageId.pid;
					
					KeyClass firstElementOfleftLeafPage = leftLeafPage.getFirst(tempRid).key;
					
					// Redistribute if left leaf is more than 50% full, redistribute
					if(leftLeafPageIdInt >= leftMostPageOfParent && leftLeafPage.getSlotCnt() > MAX_LEAF_PAGE_CAPACITY/2) {							
						// Transer all entries from current leaf page to left leaf page	
						for(tempData = currentLeafPage.getFirst(tempRid);
							tempData != null; 
							tempData = currentLeafPage.getFirst(tempRid)) {
							if(leftLeafPage.available_space() >= BT.getKeyDataLength(tempData.key, NodeType.LEAF)) {
								try {
									leftLeafPage.insertRecord(tempData);
								} catch (InsertRecException e) {
									System.out.println("Insert of record " + tempData.key + " in left leaf page"
											+ " " + leftLeafPage.getCurPage()+ " has failed during "
													+ "leafs redistribution.");
									e.printStackTrace();
								}
								currentLeafPage.deleteSortedRecord(tempRid);
							} else {
								break;
							}
						}
						
						int restElementsInCurrentNode = currentLeafPage.numberOfRecords();
						int elementsInLeftNode = leftLeafPage.numberOfRecords();
						int tempMedian = (int) Math.floor((restElementsInCurrentNode + elementsInLeftNode) / 2);
						
						// Transer 50% records of (current leaf + left leaf) entries temporary leaf page	
						for(tempData = leftLeafPage.getFirst(tempRid);
							tempData != null; 
							tempData = leftLeafPage.getFirst(tempRid)) {
							if(tempMedian>0) {
								try {
									newTempLeafPage.insertRecord(tempData);
								} catch (InsertRecException e) {
									System.out.println("Insert of record " + tempData.key + " in temporary leaf page"
											+ " " + leftLeafPage.getCurPage()+ " has failed during "
													+ "leafs redistribution.");
									e.printStackTrace();
								}
								leftLeafPage.deleteSortedRecord(tempRid);
								tempMedian--;
							} else {
								break;
							}
						}
						
						// Transfer remaining records from left leaf to current leaf page
						for(tempData = leftLeafPage.getFirst(tempRid);
								tempData != null; 
								tempData = leftLeafPage.getFirst(tempRid)) {
								try {
									currentLeafPage.insertRecord(tempData);
								} catch (InsertRecException e) {
									System.out.println("Insert of record " + tempData.key + " in current leaf page"
											+ " " + leftLeafPage.getCurPage()+ " has failed during "
													+ "leafs redistribution.");
									e.printStackTrace();
								}
								leftLeafPage.deleteSortedRecord(tempRid);
								
							}
						// Transfer all records from temporary leaf to left leaf page
     					for(tempData = newTempLeafPage.getFirst(tempRid);
							tempData != null; 
							tempData = newTempLeafPage.getFirst(tempRid)) {
							try {
								leftLeafPage.insertRecord(tempData);
							} catch (InsertRecException e) {
								System.out.println("Insert of record " + tempData.key + " in left leaf page"
										+ " " + leftLeafPage.getCurPage()+ " has failed during "
												+ "leafs redistribution.");
								e.printStackTrace();
							}
							newTempLeafPage.deleteSortedRecord(tempRid);
						}
						//adjust parent keys
     					KeyClass newFirstElementKeyOfLeftLeafPage = leftLeafPage.getFirst(tempRid).key;
						KeyClass newFirstElementKeyOfCurrentLeafPage = currentLeafPage.getFirst(tempRid).key;
						currentParentPage.adjustKey(newFirstElementKeyOfCurrentLeafPage, firstElementOfCurrentLeafPage.key);
						currentParentPage.adjustKey(newFirstElementKeyOfLeftLeafPage, firstElementOfleftLeafPage);
						return null;
					}
				}
				if(rightLeafPageId.pid != INVALID_PAGE) { // check if current leaf page has right leaf page
					BTLeafPage rightLeafPage = new BTLeafPage(rightLeafPageId, headerPage.get_keyType());
					rightLeafPageId = rightLeafPage.getCurPage();
					
					firstElementOfRightLeafPage =  rightLeafPage.getFirst(tempRid);//first element of deleted-element page
					
					KeyClass keyToFindInParent = rightLeafPage.getFirst(tempRid).key;
					KeyDataEntry keyFoundInParent = currentParentPage.findKeyData(keyToFindInParent);
					
					// If right leaf page is more than 50% full, redistribute
					if(keyFoundInParent != null && rightLeafPage.getSlotCnt() > MAX_LEAF_PAGE_CAPACITY/2) {
						//transfer records from right leaf page to current leaf page, till both are equal
						for(tempData = rightLeafPage.getFirst(tempRid);
							tempData != null; 
							tempData = rightLeafPage.getFirst(tempRid)) {
							if(currentLeafPage.available_space() >= rightLeafPage.available_space() && 
									rightLeafPage.getSlotCnt() > MAX_LEAF_PAGE_CAPACITY/2) {
								try {
									currentLeafPage.insertRecord(tempData);
								} catch (InsertRecException e) {
									System.out.println("Insert of record " + tempData.key + " in right leaf page"
											+ " " + currentLeafPage.getCurPage()+ " has failed during "
													+ "leafs redistribution.");
									e.printStackTrace();
								}
								rightLeafPage.deleteSortedRecord(tempRid);
							} else {
								break;
							}
						}			
						
						//adjust parent keys
						KeyClass newFirstElementKeyOfRightLeafPage = rightLeafPage.getFirst(tempRid).key;
						KeyClass newFirstElementKeyOfCurrentLeafPage = currentLeafPage.getFirst(tempRid).key;
						currentParentPage.adjustKey(newFirstElementKeyOfRightLeafPage, firstElementOfRightLeafPage.key);
						currentParentPage.adjustKey(newFirstElementKeyOfCurrentLeafPage, firstElementOfCurrentLeafPage.key);
						
						return null;
					}
				}
				
				// Code for merging of leaf pages begins here :
				
				if(leftLeafPageId.pid != INVALID_PAGE) {
					
					tempPage = pinPage(leftLeafPageId); //Create a new empty page and pin it
					tempSortedPage = new BTSortedPage(tempPage, leftLeafPageId.pid); //associate the SortedPage instance with Page instance
					BTLeafPage leftLeafPage = new BTLeafPage(tempSortedPage, headerPage.get_keyType());
					leftLeafPageId = leftLeafPage.getCurPage();
					
					int leftMostPageOfParent = currentParentPage.getLeftLink().pid;
					int leftLeafPageIdInt = leftLeafPageId.pid;
				
					if(leftLeafPageIdInt >= leftMostPageOfParent && leftLeafPage.getSlotCnt() == MAX_LEAF_PAGE_CAPACITY/2) {							
							
						KeyDataEntry upDataToDelete = currentLeafPage.getFirst(tempRid);
						for(tempData = currentLeafPage.getFirst(tempRid);
							tempData != null; 
							tempData = currentLeafPage.getFirst(tempRid)) {
							try {
								leftLeafPage.insertRecord(tempData);
							} catch (InsertRecException e) {
								System.out.println("Insert of record " + tempData.key + " in left leaf page"
										+ " " + leftLeafPage.getCurPage()+ " has failed during "
												+ "leafs merge.");
								e.printStackTrace();
							}
							currentLeafPage.deleteSortedRecord(tempRid);
						}
						
						if(currentLeafPage.getNextPage().pid != INVALID_PAGE) {
								PageId nextPointerOfCurrentPageId = currentLeafPage.getNextPage();
								BTSortedPage nextOfCurrentPage = new BTSortedPage(
										pinPage(nextPointerOfCurrentPageId), nextPointerOfCurrentPageId.pid);
								
								nextOfCurrentPage.setPrevPage(leftLeafPage.getCurPage());
								unpinPage(nextPointerOfCurrentPageId, true);
							}
						leftLeafPage.setNextPage(currentLeafPage.getNextPage());

						currentLeafPage.setNextPage(new PageId(INVALID_PAGE));
						currentLeafPage.setPrevPage(new PageId(INVALID_PAGE));
						
						tempData = upDataToDelete;
						
						IntegerKey intkey = (IntegerKey) tempData.key;
						KeyDataEntry leafUpDelete=new KeyDataEntry(intkey.getKey(), currentLeafPageId);
						
						currentLeafPage = null;
						
						return leafUpDelete;						
					}
				}
				
				if(rightLeafPageId.pid != INVALID_PAGE) {
					
					tempPage = pinPage(rightLeafPageId); //Create a new empty page and pin it
					tempSortedPage = new BTSortedPage(tempPage, rightLeafPageId.pid); //associate the SortedPage instance with Page instance
					BTLeafPage rightLeafPage = new BTLeafPage(tempSortedPage, headerPage.get_keyType());
					rightLeafPageId = rightLeafPage.getCurPage();
					
					KeyClass keyToFindInParent = rightLeafPage.getFirst(tempRid).key;
					KeyDataEntry keyFoundInParent = currentParentPage.findKeyData(keyToFindInParent);
					
					KeyClass firstKeyOfRightLeafPage = keyToFindInParent;
					
					if(keyFoundInParent != null && rightLeafPage.getSlotCnt() == MAX_LEAF_PAGE_CAPACITY/2) {							
							
						KeyDataEntry upDataToDelete = currentLeafPage.getFirst(tempRid);
						for(tempData = currentLeafPage.getFirst(tempRid);
							tempData != null; 
							tempData = currentLeafPage.getFirst(tempRid)) {
							try {
								rightLeafPage.insertRecord(tempData);
							} catch (InsertRecException e) {
								System.out.println("Insert of record " + tempData.key + " in right leaf page"
										+ " " + rightLeafPage.getCurPage()+ " has failed during "
												+ "leafs merge.");
								e.printStackTrace();
							}
							currentLeafPage.deleteSortedRecord(tempRid);
						}
						
						
						if(currentLeafPage.getPrevPage().pid != INVALID_PAGE) {
							PageId prevPointerOfCurrentPageId = currentLeafPage.getPrevPage();
							BTSortedPage prevOfCurrentPage = new BTSortedPage(
									pinPage(prevPointerOfCurrentPageId), prevPointerOfCurrentPageId.pid);
							
							prevOfCurrentPage.setNextPage(rightLeafPage.getCurPage());
							unpinPage(prevPointerOfCurrentPageId, true);
						} 
												
						rightLeafPage.setPrevPage(currentLeafPage.getPrevPage());
						rightLeafPage.setNextPage(rightLeafPage.getNextPage());

						if(currentLeafPage.getCurPage().pid == currentParentPage.getLeftLink().pid) {
							currentParentPage.setLeftLink(rightLeafPageId);
							KeyDataEntry leafUpDelete=new KeyDataEntry(currentParentPage.getFirst(tempRid).key, rightLeafPageId);
							
							currentLeafPage.setNextPage(new PageId(INVALID_PAGE));
							currentLeafPage.setPrevPage(new PageId(INVALID_PAGE));
							
							return leafUpDelete;
						}
						
						KeyClass newFirstElementKeyOfRightLeafPage = rightLeafPage.getFirst(tempRid).key;
						currentParentPage.adjustKey(newFirstElementKeyOfRightLeafPage, firstKeyOfRightLeafPage);
						
						tempData = upDataToDelete;
						
						IntegerKey intkey = (IntegerKey) tempData.key;
						KeyDataEntry leafUpDelete=new KeyDataEntry(intkey.getKey(), currentLeafPageId);
						
						currentLeafPage.setNextPage(new PageId(INVALID_PAGE));
						currentLeafPage.setPrevPage(new PageId(INVALID_PAGE));
						
						currentLeafPage = null;
						
						return leafUpDelete;						
					}
				}
			}
		}
		return null;
	}
	
	private BTLeafPage fullDeleteHelper(KeyClass key, RID rid)
			throws LeafDeleteException, KeyNotMatchException, PinPageException,
			ConstructPageException, IOException, UnpinPageException,
			PinPageException, IndexSearchException, IteratorException {
	
		BTLeafPage leafPage;
		KeyDataEntry keyDataEntry;
		
		leafPage = findRunStart(key, rid); //leafPage where the deletion should happen.

		if(leafPage == null) {
			//record not found, return
			return null; 
		}
		
		keyDataEntry = leafPage.getCurrent(rid); //KeyDataEntry of record to be deleted
		
		//If keyDataEntry is not null, use KeyCompare to check it is the record to be deleted
		if(BT.keyCompare(key, keyDataEntry.key) != 0) {
			return null;
		} 
		//If keyDataEntry is not null, use KeyCompare to check it is the record to be deleted
		if((leafPage.delEntry(keyDataEntry)) == true) {
			//delete successful	
			return leafPage;
		}
		unpinPage(leafPage.getCurPage());
		return null;
	}
				
	
	/*
	 * findRunStart. Status BTreeFile::findRunStart (const void lo_key, RID
	 * *pstartrid)
	 * 
	 * find left-most occurrence of `lo_key', going all the way left if lo_key
	 * is null.
	 * 
	 * Starting record returned in *pstartrid, on page *pppage, which is pinned.
	 * 
	 * Since we allow duplicates, this must "go left" as described in the text
	 * (for the search algorithm).
	 * 
	 * @param lo_key find left-most occurrence of `lo_key', going all the way
	 * left if lo_key is null.
	 * 
	 * @param startrid it will reurn the first rid =< lo_key
	 * 
	 * @return return a BTLeafPage instance which is pinned. null if no key was
	 * found.
	 */

	BTLeafPage findRunStart(KeyClass lo_key, RID startrid) throws IOException,
			IteratorException, KeyNotMatchException, ConstructPageException,
			PinPageException, UnpinPageException {
		BTLeafPage pageLeaf;
		BTIndexPage pageIndex;
		Page page;
		BTSortedPage sortPage;
		PageId pageno;
		PageId curpageno = null; // iterator
		PageId prevpageno;
		PageId nextpageno;
		RID curRid;
		KeyDataEntry curEntry;

		pageno = headerPage.get_rootId();

		if (pageno.pid == INVALID_PAGE) { // no pages in the BTREE
			pageLeaf = null; // should be handled by
			// startrid =INVALID_PAGEID ; // the caller
			return pageLeaf;
		}

		page = pinPage(pageno);
		sortPage = new BTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + pageno + lineSep);
			trace.flush();
		}

		// ASSERTION
		// - pageno and sortPage is the root of the btree
		// - pageno and sortPage valid and pinned

		while (sortPage.getType() == NodeType.INDEX) {
			pageIndex = new BTIndexPage(page, headerPage.get_keyType());
			prevpageno = pageIndex.getPrevPage();
			curEntry = pageIndex.getFirst(startrid);
			while (curEntry != null && lo_key != null
					&& BT.keyCompare(curEntry.key, lo_key) < 0) {

				prevpageno = ((IndexData) curEntry.data).getData();
				curEntry = pageIndex.getNext(startrid);
			}

			unpinPage(pageno);

			pageno = prevpageno;
			page = pinPage(pageno);
			sortPage = new BTSortedPage(page, headerPage.get_keyType());

			if (trace != null) {
				trace.writeBytes("VISIT node " + pageno + lineSep);
				trace.flush();
			}

		}

		pageLeaf = new BTLeafPage(page, headerPage.get_keyType());

		curEntry = pageLeaf.getFirst(startrid);
		while (curEntry == null) {
			// skip empty leaf pages off to left
			nextpageno = pageLeaf.getNextPage();
			unpinPage(pageno);
			if (nextpageno.pid == INVALID_PAGE) {
				// oops, no more records, so set this scan to indicate this.
				return null;
			}

			pageno = nextpageno;
			pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());
			curEntry = pageLeaf.getFirst(startrid);
		}

		// ASSERTIONS:
		// - curkey, curRid: contain the first record on the
		// current leaf page (curkey its key, cur
		// - pageLeaf, pageno valid and pinned

		if (lo_key == null) {
			return pageLeaf;
			// note that pageno/pageLeaf is still pinned;
			// scan will unpin it when done
		}

		while (BT.keyCompare(curEntry.key, lo_key) < 0) {
			curEntry = pageLeaf.getNext(startrid);
			while (curEntry == null) { // have to go right
				nextpageno = pageLeaf.getNextPage();
				unpinPage(pageno);

				if (nextpageno.pid == INVALID_PAGE) {
					return null;
				}

				pageno = nextpageno;
				pageLeaf = new BTLeafPage(pinPage(pageno),
						headerPage.get_keyType());

				curEntry = pageLeaf.getFirst(startrid);
			} 
		}
		if(BT.keyCompare(curEntry.key, lo_key) > 0) {
			return null;
		}

		return pageLeaf;
	}

	/*
	 * Status BTreeFile::NaiveDelete (const void *key, const RID rid)
	 * 
	 * Remove specified data entry (<key, rid>) from an index.
	 * 
	 * We don't do merging or redistribution, but do allow duplicates.
	 * 
	 * Page containing first occurrence of key `key' is found for us by
	 * findRunStart. We then iterate for (just a few) pages, if necesary, to
	 * find the one containing <key,rid>, which we then delete via
	 * BTLeafPage::delUserRid.
	 */

	private boolean NaiveDelete(KeyClass key, RID rid)
			throws LeafDeleteException, KeyNotMatchException, PinPageException,
			ConstructPageException, IOException, UnpinPageException,
			PinPageException, IndexSearchException, IteratorException {
	
		BTLeafPage leafPage;
		RID rIdIterator = new RID();
		KeyDataEntry keyDataEntry;
		
		leafPage = findRunStart(key, rid); //leafPage where the deletion should happen.

		if(leafPage == null) {
			//record not found
			return false; 
		}
		keyDataEntry = leafPage.getCurrent(rid); //KeyDataEntry of record to be deleted
		
		while(true) {
			while (keyDataEntry == null ) {
				//If keyDataEntry is null, get NextPage
				PageId nextPageId = leafPage.getNextPage();
				unpinPage(leafPage.getPrevPage());
				if(nextPageId.pid == INVALID_PAGE) {
					//If nextPage is last page, return false
					return false;
				}
				//If nextPage is not last page, associate BTLeafPage instance with the nextPage instance
				leafPage =  new BTLeafPage(pinPage(nextPageId), 
						headerPage.get_keyType());	
				keyDataEntry = leafPage.getFirst(rIdIterator);
			}
			//If keyDataEntry is not null, use KeyCompare to check it is the record to be deleted
			if(BT.keyCompare(key, keyDataEntry.key) > 0) {
				break;
			} 
			if((leafPage.delEntry(keyDataEntry)) == true) {
				//delete successful
				unpinPage(leafPage.getCurPage(), true);
				return true;
			}
			
			PageId rightPageId = leafPage.getNextPage();
			unpinPage(leafPage.getCurPage());
			leafPage =  new BTLeafPage(pinPage(rightPageId), 
					headerPage.get_keyType());
			keyDataEntry = leafPage.getFirst(rIdIterator);
		}
		unpinPage(leafPage.getCurPage());
		return false;
	}

	/**
	 * create a scan with given keys Cases: (1) lo_key = null, hi_key = null
	 * scan the whole index (2) lo_key = null, hi_key!= null range scan from min
	 * to the hi_key (3) lo_key!= null, hi_key = null range scan from the lo_key
	 * to max (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match (
	 * might not unique) (5) lo_key!= null, hi_key!= null, lo_key < hi_key range
	 * scan from lo_key to hi_key
	 *
	 * @param lo_key
	 *            the key where we begin scanning. Input parameter.
	 * @param hi_key
	 *            the key where we stop scanning. Input parameter.
	 * @exception IOException
	 *                error from the lower layer
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception UnpinPageException
	 *                error when unpin a page
	 */
	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
			throws IOException, KeyNotMatchException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException

	{
		BTFileScan scan = new BTFileScan();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			scan.leafPage = null;
			return scan;
		}

		scan.treeFilename = dbname;
		scan.endkey = hi_key;
		scan.didfirst = false;
		scan.deletedcurrent = false;
		scan.curRid = new RID();
		scan.keyType = headerPage.get_keyType();
		scan.maxKeysize = headerPage.get_maxKeySize();
		scan.bfile = this;

		// this sets up scan at the starting position, ready for iteration
		scan.leafPage = findRunStart(lo_key, scan.curRid);
		return scan;
	}

	void trace_children(PageId id) throws IOException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException {

		if (trace != null) {

			BTSortedPage sortedPage;
			RID metaRid = new RID();
			PageId childPageId;
			KeyClass key;
			KeyDataEntry entry;
			sortedPage = new BTSortedPage(pinPage(id), headerPage.get_keyType());

			// Now print all the child nodes of the page.
			if (sortedPage.getType() == NodeType.INDEX) {
				BTIndexPage indexPage = new BTIndexPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
				trace.writeBytes(" " + indexPage.getPrevPage());
				for (entry = indexPage.getFirst(metaRid); entry != null; entry = indexPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + ((IndexData) entry.data).getData());
				}
			} else if (sortedPage.getType() == NodeType.LEAF) {
				BTLeafPage leafPage = new BTLeafPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
				for (entry = leafPage.getFirst(metaRid); entry != null; entry = leafPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + entry.key + " " + entry.data);
				}
			}
			unpinPage(id);
			trace.writeBytes(lineSep);
			trace.flush();
		}

	}

}
