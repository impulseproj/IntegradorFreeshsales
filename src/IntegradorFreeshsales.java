import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class IntegradorFreeshsales {
	
	private List<String> listFieldsContact = new ArrayList<String>();
	private List<String> listFieldsSales = new ArrayList<String>();
	private List<String> listFieldsLeadsActivites = new ArrayList<String>();
	private List<String> listFieldsAccount = new ArrayList<String>();
	private List<String> listFieldsLead = new ArrayList<String>();
	private List<String> listFieldsDeal = new ArrayList<String>();
	private List<String> listFieldsDealDate = new ArrayList<String>();
	private List<String> listFieldsLeadDate = new ArrayList<String>();
	private List<String> listFieldsCompany = new ArrayList<String>();
	private List<String> listFieldsCustom = new ArrayList<String>();
	private List<String> listFieldsCustomContact  = new ArrayList<String>();
	private List<String> listFieldsCustomDeal  = new ArrayList<String>();
	private HashMap<String,List<String>> mapContatoDeal = new HashMap<String, List<String>>();
	
	
	
	private HashMap<String,Deal> mapDealContato = new HashMap<String, Deal>();
	
	private FileWriter arquivoLog = null;
	private String local = "C:/IntegradorFreshsales/logs/";
	private Connection conexao = null;
	
	private JSONArray jsonObjectUsers = null;
	private  JSONArray jsonObjectLeadStages =  null;
	private  JSONArray jsonObjectLeadReason =  null;
	private  JSONArray jsonObjectLeadSource =  null;
	private  JSONArray jsonObjectTerritories =  null;
	private  JSONArray jsonObjectCampaigns =  null;
	
	public void executaTeste() {
		
		preparaVariaveisDefault();
		carregaConexaoTeste();
		
		String id = "3012651919";
		
		
		
		
		
		
		JSONParser parser = new JSONParser();
		HashMap<String, List<Object>> list = new HashMap<String, List<Object>>();
		
	
		
		try {
			String u = "https://impulse.freshsales.io/api/contacts/"+id+"/activities?include=user&page=3";
         	URL url = new URL(u);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Token token=wPBMcHQSK9yfCi3omDVGUQ");

			if (conn.getResponseCode() == 200) {
			
				  BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
	              String output;
	              StringBuffer strBuffer = new StringBuffer();
				  while ((output = br.readLine()) != null) {
					  strBuffer.append(output);
				  }
				 // System.out.println(strBuffer.toString());
				  Object obj =  parser.parse(strBuffer.toString());
				  JSONObject jsonObject = (JSONObject) obj;
				  String colunasInsert = "";
				  String  valoresInsert = "";
				  String virgula = "";
				  List<Object>  listValores = new ArrayList<Object>();
				
				  JSONArray jsonObjectLeads = (JSONArray) jsonObject.get("activities");
				  for(int j = 0 ; j < jsonObjectLeads.size(); j++) {
					  
					  colunasInsert = "";
					   valoresInsert = "";
					   virgula = "";
					   listValores = new ArrayList<Object>();
					  
					  JSONObject jsonObjectLead = (JSONObject)jsonObjectLeads.get(j);
					  
					  for(int i = 0; i < listFieldsLeadsActivites.size(); i++) {
						  String coluna = listFieldsLeadsActivites.get(i);
						  coluna = coluna.trim();
						  
						  try {
							  if(coluna.equalsIgnoreCase("ac_created_at")) {
								  coluna = "created_at";
							  }
							  
							  Object value = (Object)jsonObjectLead.get(coluna);
							  
							  if(coluna.equalsIgnoreCase("created_at")) {
								  coluna = "ac_created_at";
							  }
							  
						      colunasInsert = colunasInsert + virgula + coluna;
						      valoresInsert = valoresInsert + virgula + "?";
						      virgula = ",";
						      if(value != null)
						         listValores.add(value.toString());
						      else {
						    	  value = "null";
						    	  if(coluna.equalsIgnoreCase("ac_created_at")) 
						    	       value = "NULL"; 
						    	  
						    	  listValores.add(value);
						      }
						      
						   
						      
						      
						  }catch (Exception e) {}
						  
					  }
					  
					  
					  
					  try {
						  JSONObject jsonObjectAction = (JSONObject)jsonObjectLead.get("action_data"); 
						  
						  
						  List<String> listAc = new ArrayList<String>();
						  listAc.add("email_id");
						  listAc.add("subject");
						  listAc.add("name");
						  listAc.add("count");
						  listAc.add("first_occurrence");
						  listAc.add("last_occurrence");
						  listAc.add("property_key");
						  listAc.add("property_value");
						  listAc.add("description");
						  listAc.add("end_date");
						  listAc.add("from_date");
						  listAc.add("time_zone");
						  listAc.add("title");
						  listAc.add("stage_id");
						  listAc.add("stage_name");
						  listAc.add("recording_url");
						  listAc.add("recording_duration");
						  listAc.add("outcome");
						  
						  for(int l=0;l<listAc.size();l++) {
							  String key = listAc.get(l);
							  
							  try { Object value = (Object)jsonObjectAction.get(key);
							  
							  colunasInsert = colunasInsert + virgula + "ac_data_"+key;
						      valoresInsert = valoresInsert + virgula + "?";
						      virgula = ",";
						      if(value != null)
						         listValores.add(value.toString());
						      else {
						    	  value = "null";
						    	  if(key.equalsIgnoreCase("first_occurrence")) {
						    		  value = "NULL"; 
						    	  }
						    	  if(key.equalsIgnoreCase("last_occurrence")) {
						    		  value = "NULL"; 
						    	  }
						    	  
						    	  listValores.add(value);
						      }
							         
							  }catch (Exception e) {}
							  
							  
							 
							  
						  }
						  
						  try {
							  JSONObject jsonObjectActionNumber = (JSONObject)jsonObjectAction.get("phone_caller"); 
							  
							  
							  Object value = (Object)jsonObjectActionNumber.get("number");
							  
							  colunasInsert = colunasInsert + virgula + " ac_data_phone_caller_number";
						      valoresInsert = valoresInsert + virgula + "?";
						      virgula = ",";
						      if(value != null)
						         listValores.add(value.toString());
						      else {
						    	  value = "null";
						    	  listValores.add(value);
						      }
							         
							  }catch (Exception e) {}
						  
						  
						 
											  
						  
					  }catch (Exception e) {
						
						  try {
							  Object value = (Object)jsonObjectLead.get("action_data");
						      colunasInsert = colunasInsert + virgula + "action_data";
						      valoresInsert = valoresInsert + virgula + "?";
						      virgula = ",";
						      if(value != null)
						         listValores.add(value.toString());
						      else {
						    	  value = "null";
						    	  listValores.add(value);
						      }
						      
						   
						      
						      
						  }catch (Exception ex) {}
						  
					}
					 
					  
					  
					  
					 
					  String insert = "insert into freshsales_leads_activities ("+colunasInsert+", lead_id) values ("+valoresInsert+", '"+id+"')!#!"+UUID.randomUUID();
						
					  List<Object> listAux = new ArrayList<Object>();
						 for(int i = 0 ; i < listValores.size(); i++) {
							 String str = listValores.get(i).toString();
							 if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-03:00") != -1) {
								 str = str.replaceAll("T", " ");
								 str = str.replaceAll("-03:00", "");
								 str = str.trim();
								 listAux.add(str); 
							 }else if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-02:00") != -1) {
								 str = str.replaceAll("T", " ");
								 str = str.replaceAll("-02:00", "");
								 str = str.trim();
								 listAux.add(str); 
							 }else {
								 listAux.add(str); 
							 }
						 }
						 listValores = new ArrayList<Object>();
						 for(int i = 0 ; i < listAux.size(); i++) {
							 listValores.add(listAux.get(i)); 
						 }
					  
					  
					  list.put(insert, listValores);
					  //insereContato(insert, listValores, id);
					  
				  }
				  
				  
				  HashMap<String, List<Object>> listIdsLeads = new HashMap<String, List<Object>>();
				  Iterator<String> it = list.keySet().iterator();
				  while(it.hasNext()) {
					  String key = it.next();
					  listIdsLeads.put(key, list.get(key));
				  }
				  
				  //this.deleteactivites(id);
					 Iterator<String> it2 = listIdsLeads.keySet().iterator();
					 while(it2.hasNext()) {
						String key =  it2.next();
						List<Object> list3 = listIdsLeads.get(key);
						insereLead(key, list3);
					 }
				  
				  System.out.println("");
			  
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	public void carregaSalesActive() {
		
		List<String> listToken = new ArrayList<String>();
		listToken.add("MwU8JEpSnWr65tMhY83_xg");
		listToken.add("c-yNJQkhjwx2_rNE66776w");
		listToken.add("XQnWvBOwR8fx50p1dK0f2Q");
		listToken.add("JYV7gu_5IucuV1LtQMdu_Q");
		listToken.add("5kBHqHBHvg8ZkweToeaczQ");
		
		listToken.add("tcUsOWxtVesTyJjjC9VgMg");
		listToken.add("Q-AwMqEemHOcMXUrsBH1jA");
		listToken.add("5QZGSXph69KfEQ_gMjrolw");
		listToken.add("RobrmKaReY76kdmTe5gnCA");
		listToken.add("IB9nlL1DFWtJKmcJJ4Yd-A");
		
		
		
		
		escreveLog("Carregando configuration:  "+new Date());
		String method = "";
		List<String> listCampos = new ArrayList<String>();
		String tabela = "";
		
		method = "sales_activity_types";
		tabela = "freshsales_sales_activity_types";
		listCampos.add("partial");
		listCampos.add("id");
		listCampos.add("name");
		listCampos.add("internal_name");
		listCampos.add("show_in_conversation");
		listCampos.add("position");
		listCampos.add("is_default");
		listCampos.add("is_checkedin");
		HashMap<String, List<Object>> listConfiguration = this.listConfiguration(method, listCampos,  tabela);
		 if(listConfiguration.size() > 0) {
			 escreveLog("Inserindo "+tabela+" : "+new Date());
			 this.deleteconfiguration(tabela);
			 Iterator<String> it = listConfiguration.keySet().iterator();
			 while(it.hasNext()) {
				String key =  it.next();
				List<Object> list = listConfiguration.get(key);
				insereLead(key, list);
			 }
			 
			 escreveLog("Fim "+tabela+" : "+new Date());
		 }
		
    	listCampos = new ArrayList<String>();
		method = "sales_activity_entity_types";
		tabela = "freshsales_sales_activity_entity_types";
		listCampos.add("partial");
		listCampos.add("id");
		listCampos.add("name");
		listCampos.add("sales_activity_type_id");
		listCampos.add("sales_activity_type_name");
		listCampos.add("position");
		 listConfiguration = this.listConfiguration(method, listCampos,  tabela);
		 if(listConfiguration.size() > 0) {
			 escreveLog("Inserindo "+tabela+" : "+new Date());
			 this.deleteconfiguration(tabela);
			 Iterator<String> it = listConfiguration.keySet().iterator();
			 while(it.hasNext()) {
				String key =  it.next();
				List<Object> list = listConfiguration.get(key);
				insereLead(key, list);
			 }
			 
			 escreveLog("Fim "+tabela+" : "+new Date());
		 }
		
		
		
		listCampos = new ArrayList<String>();
		method = "sales_activity_outcomes";
		tabela = "freshsales_sales_activity_outcomes";
		
		listCampos.add("partial");
		listCampos.add("id");
		listCampos.add("name");
		listCampos.add("sales_activity_type_id");
		listCampos.add("position");
		 listConfiguration = this.listConfiguration(method, listCampos,  tabela);
		 if(listConfiguration.size() > 0) {
			 escreveLog("Inserindo "+tabela+" : "+new Date());
			 this.deleteconfiguration(tabela);
			 Iterator<String> it = listConfiguration.keySet().iterator();
			 while(it.hasNext()) {
				String key =  it.next();
				List<Object> list = listConfiguration.get(key);
				insereLead(key, list);
			 }
			 
			 escreveLog("Fim "+tabela+" : "+new Date());
		 }
		
		 escreveLog("FIM Carregando configuration:  "+new Date());
		
		
		
		for(int i = 0; i < listToken.size(); i++) {
			 String token = listToken.get(i);
			 
			 String owner_display_name = "";
			 String owner_email = "";
			 
			 if(token.equals("MwU8JEpSnWr65tMhY83_xg")) {
				  owner_display_name = "Micaelle Siqueira";
				  owner_email = "micaelle.siqueira@impulseup.com";
			 }
			 else  if(token.equals("c-yNJQkhjwx2_rNE66776w")) {
				  owner_display_name = "Renata Caramaschi";
				  owner_email = "renata.caramaschi@impulseup.com";
			 }
			 else  if(token.equals("XQnWvBOwR8fx50p1dK0f2Q")) {
				  owner_display_name = "Mayra Castro";
				  owner_email = "mayra.castro@impulseup.com";
			 }
			 else  if(token.equals("JYV7gu_5IucuV1LtQMdu_Q")) {
				  owner_display_name = "Bernardo José";
				  owner_email = "bernardo.jose@impulseup.com";
			 }
			 else  if(token.equals("5kBHqHBHvg8ZkweToeaczQ")) {
				  owner_display_name = "Luis Queiroz";
				  owner_email = "luis.queiroz@impulseup.com";
			 }
			 
			 
			 else  if(token.equals("tcUsOWxtVesTyJjjC9VgMg")) {
				  owner_display_name = "Ana Almeida";
				  owner_email = "ana.almeida@impulseup.com";
			 }
			 else  if(token.equals("Q-AwMqEemHOcMXUrsBH1jA")) {
				  owner_display_name = "Anna Paula";
				  owner_email = "anna.paula@impulseup.com";
			 }
			 else  if(token.equals("5QZGSXph69KfEQ_gMjrolw")) {
				  owner_display_name = "João Lopes";
				  owner_email = "joao.lopes@impulseup.com";
			 }
			 else  if(token.equals("RobrmKaReY76kdmTe5gnCA")) {
				  owner_display_name = "Izabela Fam";
				  owner_email = "izabela.fam@impulseup.com";
			 }
			 else  if(token.equals("IB9nlL1DFWtJKmcJJ4Yd-A")) {
				  owner_display_name = "Karynne Rocha";
				  owner_email = "karynne.rocha@impulseup.com";
			 }
			 
		 
			 
		
			 escreveLog("Carregando sales active:  "+owner_display_name+"  : "+new Date());
			 HashMap<String, List<Object>> listAllSales = this.getAllSales(token, owner_display_name, owner_email);
			 escreveLog("Terminou sales active:  "+owner_display_name+"   : "+new Date());
			 if(listAllSales.size() > 0) {
				 escreveLog("Inserindo sales active "+owner_display_name+" : "+new Date());
				 this.deletesales(token);
				 Iterator<String> it = listAllSales.keySet().iterator();
				 while(it.hasNext()) {
					String key =  it.next();
					List<Object> list = listAllSales.get(key);
					insereSales(key, list);
				 }
				 
				 escreveLog("Fim salesactive insert "+owner_display_name+" : "+new Date());
			 }
		
		}
		
		
		
		
		
	}
	
	public void executa() {
		
		 preparaVariaveisDefault();
		 
		 
		 
		
		 escreveLog("Inicio processamento  : "+new Date());
		 
		 //voltar a senha pra subir depois
		 carregaConexao();
		
		 
		
		 escreveLog("Carregando lista dos contatos  : "+new Date());
		 HashMap<String, List<Object>> listAllContactIds = this.getAllContacts();
		 escreveLog("Terminou lista dos contatos  : "+new Date());
		 
		 System.out.println("Total de contatos carregados : "+listAllContactIds.size());
		 
		 if(listAllContactIds.size() > 0) {
			 escreveLog("Inserindo contatos : "+new Date());
			 this.deletecontact();
			 Iterator<String> it = listAllContactIds.keySet().iterator();
			 while(it.hasNext()) {
				String key =  it.next();
				List<Object> list = listAllContactIds.get(key);
				insereContato(key, list);
			 }
			 
			 escreveLog("Fim contatos : "+new Date());
		 }
		 
		   
 
		 escreveLog("Carregando lista dos leads  : "+new Date());
		 List<String> listIds = new ArrayList<String>();
	     HashMap<String, List<Object>> listAllLeadsIds = this.getAllLeads(listIds);
		 escreveLog("Terminou lista dos leads  : "+new Date());
		 escreveLog("Total de leads  : "+listAllLeadsIds.size());
		 if(listAllLeadsIds.size() > 0) {
			 escreveLog("Inserindo leads : "+new Date());
			 Iterator<String> it = listAllLeadsIds.keySet().iterator();
			 while(it.hasNext()) {
				String key =  it.next();
				List<Object> list = listAllLeadsIds.get(key);
				insereLead(key, list);
			 }
			 escreveLog("Fim leads : "+new Date());
		 }
		 
		
	
		
		 escreveLog("Carregando lista dos activites/leads  : "+new Date());
		 for(int i=0; i <listIds.size(); i++) {
			 String id = listIds.get(i);
			 HashMap<String, List<Object>> listAllActivites = this.getAllActivites(id);
			 if(listAllActivites.size() > 0) {
				 escreveLog("Inserindo activites/leads "+id+" : "+new Date());
				 this.deleteactivites(id);
				 Iterator<String> it = listAllActivites.keySet().iterator();
				 while(it.hasNext()) {
					String key =  it.next();
					List<Object> list = listAllActivites.get(key);
					insereLead(key, list);
				 }
				 
				 escreveLog("Fim activites/leads "+id+" : "+new Date());
			 }
			 
		 }
		 
		 //corrigindo os casos de created_at igual a null
		 List<String> listIdsOutros = new ArrayList<String>();
		 listIdsOutros = this.carregaLeadNull(listIds);
		 listIds = new ArrayList<String>();
		 listIds = listIdsOutros;
		 for(int i=0; i <listIds.size(); i++) {
			 String id = listIds.get(i);
			 HashMap<String, List<Object>> listAllActivites = this.getAllActivites(id);
			 if(listAllActivites.size() > 0) {
				 escreveLog("Inserindo activites/leads "+id+" : "+new Date());
				 this.deleteactivites(id);
				 Iterator<String> it = listAllActivites.keySet().iterator();
				 while(it.hasNext()) {
					String key =  it.next();
					List<Object> list = listAllActivites.get(key);
					insereLead(key, list);
				 }
				 
				 escreveLog("Fim activites/leads "+id+" : "+new Date());
			 }
			 
		 }
		 
		 
		 escreveLog("Terminou lista dos activites/leads  : "+new Date());
	
		 
		 escreveLog("Carregando lista dos Accounts  : "+new Date());
		 HashMap<String, SalesAccount> listAllAccounts = this.getAllAccounts();
		 escreveLog("Terminou lista dos Accounts  : "+new Date());
		 escreveLog("Carregando lista dos deals  : "+new Date());
		 HashMap<String, List<Object>> listAllDeals = this.getAllDeals1(listAllAccounts);
		 
		 HashMap<String, List<Object>> listAllDeals2 = this.getAllDeals2(listAllAccounts);
		 
		 Iterator<String> it2 = listAllDeals2.keySet().iterator();
		 while(it2.hasNext()) {
			 String key = it2.next();
			 List<Object> list = listAllDeals2.get(key);
			 listAllDeals.put(key, list);
		 }
		 
		 escreveLog("Terminou lista dos deals  : "+new Date());
		 if(listAllDeals.size() > 0) {
			 escreveLog("Inserindo deals : "+new Date());
			 this.deletedeal();
			 Iterator<String> it = listAllDeals.keySet().iterator();
			 while(it.hasNext()) {
				String key =  it.next();
				List<Object> list = listAllDeals.get(key);
				insereDeal(key, list);
			 }
			 escreveLog("Fim deals : "+new Date());
		 }
		
		 escreveLog("Atualizando o deal nos contatos  : "+new Date());
		 Iterator<String> it = mapContatoDeal.keySet().iterator();
		 while(it.hasNext()) {
			 String idDeal = it.next();
			 List<String> list = mapContatoDeal.get(idDeal);
			 for(int i = 0 ; i < list.size(); i++) {
				 String idContato = list.get(i);
				 String name = "null";
				 String amount = "null";
				 if(mapDealContato.containsKey(idDeal)) {
					 Deal deal = mapDealContato.get(idDeal);
					 name = deal.getName();
					 amount = deal.getAmount();
				 }
				 this.updateContato(idContato, idDeal, name, amount);
			 }
		 }
		 
		 escreveLog("Fim atualizacao dos contatos : "+new Date());
		 
		 
		 escreveLog("Atualizando o contato antigo no deal  : "+new Date());
		 it = mapContatoDeal.keySet().iterator();
		 while(it.hasNext()) {
			 String idDeal = it.next();
			 List<String> list = mapContatoDeal.get(idDeal);
			 String ids = "";
			 String virg = "";
			 for(int i = 0 ; i < list.size(); i++) {
				 String idContato = list.get(i);
				 ids = ids + virg + "'"+idContato+"'";
				 virg = ",";
			 }
			 //pega o contato mais antigo pra atualizar
			 this.carregaContatoAntigo(ids, idDeal);
		 }
		 
		 escreveLog("Fim atualizacao do contato no deal : "+new Date());  
		 
		 
		
		
		 escreveLog("Processou tudo  : "+new Date());
		
		 //fecha a conexao com o banco
		try {
			conexao.close();
			conexao = null;
		} catch (SQLException e) {}
		
	}
	
	/**
	 * 
	 */
	public void preparaVariaveisDefault() {
		
		listFieldsContact = new ArrayList<String>();
		listFieldsLeadsActivites = new ArrayList<String>();
		listFieldsSales = new ArrayList<String>();
		listFieldsCustomContact  = new ArrayList<String>();
		listFieldsCustomDeal  = new ArrayList<String>();
		listFieldsLead = new ArrayList<String>();
		listFieldsCompany = new ArrayList<String>();
		listFieldsCustom = new ArrayList<String>();
		listFieldsAccount = new ArrayList<String>();
		listFieldsDeal = new ArrayList<String>();
		listFieldsDealDate = new ArrayList<String>();
		mapContatoDeal = new HashMap<String, List<String>>();
		listFieldsLeadDate = new ArrayList<String>();
		
		
		listFieldsLeadDate.add("created_at");
		listFieldsLeadDate.add("last_contacted");
		listFieldsLeadDate.add("last_contacted_via_sales_activity");
		listFieldsLeadDate.add("last_contacted_via_chat");
		listFieldsLeadDate.add("last_seen");
		listFieldsLeadDate.add("updated_at");
		
		
		listFieldsLeadDate.add("last_assigned_at");
		//listFieldsLeadDate.add("last_contacted_sales_activity_mode");
		listFieldsLeadDate.add("last_contacted_via_chat");
		listFieldsLeadDate.add("stage_updated_time");
		listFieldsLeadDate.add("cf_data_de_retorno_standby");
	
		
		
		listFieldsDealDate.add("closed_date");
		listFieldsDealDate.add("created_at");
		listFieldsDealDate.add("expected_close");
		listFieldsDealDate.add("stage_updated_time");
		listFieldsDealDate.add("upcoming_activities_time");
		listFieldsDealDate.add("updated_at");
		listFieldsDealDate.add("deal_stage_updated_at");
		listFieldsDealDate.add("sales_account_last_contacted");
		listFieldsDealDate.add("sales_account_updated_at");
		listFieldsDealDate.add("contact_created_at");
		listFieldsDealDate.add("contact_last_assigned_at");
		listFieldsDealDate.add("contact_last_contacted_via_sales_activity");
		listFieldsDealDate.add("contact_last_seen");
		listFieldsDealDate.add("contact_updated_at");
		
		
		listFieldsDeal.add("active_sales_sequences");
		listFieldsDeal.add("age");
		listFieldsDeal.add("amount");
		listFieldsDeal.add("base_currency_amount");
		listFieldsDeal.add("closed_date");
		listFieldsDeal.add("completed_sales_sequences");
		listFieldsDeal.add("created_at");
		listFieldsDeal.add("created_id");
		listFieldsDeal.add("deal_pipeline_id");
		listFieldsDeal.add("expected_close");
		listFieldsDeal.add("id");
		listFieldsDeal.add("last_assigned_at");
		listFieldsDeal.add("last_contacted_sales_activity_mode");
		listFieldsDeal.add("last_contacted_via_sales_activity");
		listFieldsDeal.add("name");
		listFieldsDeal.add("probability");
		listFieldsDeal.add("recent_note");
		listFieldsDeal.add("stage_updated_time");
		listFieldsDeal.add("upcoming_activities_time");
		listFieldsDeal.add("updated_at");
		listFieldsDeal.add("updated_id");
		listFieldsDeal.add("web_form_id");
		listFieldsDeal.add("owner_id");
		listFieldsDeal.add("territory_id");
		listFieldsDeal.add("campaign_id");
		listFieldsDeal.add("lead_source_id");
		listFieldsDeal.add("deal_stage_id");
		listFieldsDeal.add("deal_reason_id");
		listFieldsDeal.add("deal_type_id");
		listFieldsDeal.add("sales_account_id");
		listFieldsCustomDeal.add("cf_problema_do_cliente");
		listFieldsCustomDeal.add("cf_quantidade_de_creditos");
		
		
		listFieldsLead.add("deal");
		listFieldsLead.add("department");
		listFieldsLead.add("display_name");
		listFieldsLead.add("do_not_disturb");
		listFieldsLead.add("duplicates_searched_today");
		listFieldsLead.add("email");
		listFieldsLead.add("email_status");
		listFieldsLead.add("facebook");
		listFieldsLead.add("first_name");
		listFieldsLead.add("has_authority");
		listFieldsLead.add("has_connections");
		listFieldsLead.add("has_duplicates");
		listFieldsLead.add("id");
		listFieldsLead.add("job_title");
		listFieldsLead.add("keyword");
		listFieldsLead.add("last_assigned_at");
		listFieldsLead.add("last_contacted");
		listFieldsLead.add("last_contacted_mode");
		listFieldsLead.add("last_contacted_sales_activity_mode");
		listFieldsLead.add("last_contacted_via_chat");
		listFieldsLead.add("last_contacted_via_sales_activity");
		listFieldsLead.add("last_name");
		listFieldsLead.add("last_seen");
		listFieldsLead.add("lead_quality");
		listFieldsLead.add("lead_score");
		listFieldsLead.add("linkedin");
		listFieldsLead.add("MEDIUM");
		listFieldsLead.add("mobile_number");
		listFieldsLead.add("recent_note");
		listFieldsLead.add("stage_updated_time");
		listFieldsLead.add("state");
		listFieldsLead.add("twitter");
		listFieldsLead.add("updated_at");
		listFieldsLead.add("updater_id");
		listFieldsLead.add("web_form_ids");
		listFieldsLead.add("work_number");
		listFieldsLead.add("zipcode");
		listFieldsLead.add("completed_sales_sequences");
		listFieldsLead.add("country");
		listFieldsLead.add("created_at");
		listFieldsLead.add("creater_id");
		listFieldsLead.add("active_sales_sequences");
		listFieldsLead.add("address");
		listFieldsLead.add("avatar");
		listFieldsLead.add("city");
		listFieldsLead.add("owner_id");
		listFieldsLead.add("lead_stage_id");
		listFieldsLead.add("lead_reason_id");
		listFieldsLead.add("lead_source_id");
		listFieldsLead.add("territory_id");
		listFieldsLead.add("campaign_id");
		listFieldsLead.add("tags");
		
		
		listFieldsCompany.add("address");
		listFieldsCompany.add("annual_revenue");
		listFieldsCompany.add("business_type");
		listFieldsCompany.add("business_type_id");
		listFieldsCompany.add("city");
		listFieldsCompany.add("country");
		listFieldsCompany.add("id");
		listFieldsCompany.add("industry_type");
		listFieldsCompany.add("industry_type_id");
		listFieldsCompany.add("name");
		listFieldsCompany.add("number_of_employees");
		listFieldsCompany.add("phone");
		listFieldsCompany.add("state");
		listFieldsCompany.add("website");
		listFieldsCompany.add("zipcode");
		
		
		
		listFieldsCustom.add("cf_solicitou_trial");
		listFieldsCustom.add("cf_acessou_etapa_2");
		listFieldsCustom.add("cf_acessou_etapa_3");
		listFieldsCustom.add("cf_acessou_etapa_4");
		listFieldsCustom.add("cf_acessou_analytics");
		listFieldsCustom.add("cf_acessou_nine-box");
		listFieldsCustom.add("cf_acessou_pdi");
		listFieldsCustom.add("cf_acessou_okr");
		listFieldsCustom.add("cf_demo_site");
		listFieldsCustom.add("cf_acessou_acompanhamento");
		listFieldsCustom.add("cf_visualizou_creditos"); 
		listFieldsCustom.add("cf_telefone_invalido");
		listFieldsCustom.add("cf_erro_de_cadastro");
		
		listFieldsCustom.add("cf_calculou_creditos");
		listFieldsCustom.add("cf_comprou_creditos"); 
		listFieldsCustom.add("cf_converteu_site");   
		listFieldsCustom.add("cf_converteu_trial");  
		listFieldsCustom.add("cf_criou_avaliacao");   
		listFieldsCustom.add("cf_demo");
		listFieldsCustom.add("cf_demo_realizada");  
		listFieldsCustom.add("cf_e-mail_alternativo");
		listFieldsCustom.add("cf_engajado");  
		listFieldsCustom.add("cf_enviou_avaliacao");  
		listFieldsCustom.add("cf_link_rd");
		listFieldsCustom.add("cf_recebeu_invite");    
		listFieldsCustom.add("cf_solicitou_consultor");   
		listFieldsCustom.add("cf_solicitou_proposta");   
		listFieldsCustom.add("cf_target_sales_campaign");
		listFieldsCustom.add("cf_visualizou_relatorio"); 
		listFieldsCustom.add("cf_colaboradores");
		listFieldsCustom.add("cf_cadastro_manual_-_erro_integracao");
		
		
		listFieldsCustom.add("cf_converteu_trial_okr");
		listFieldsCustom.add("cf_solicitou_trial_okr");
	
		listFieldsCustom.add("cf_acessou_resultados_adm");
		listFieldsCustom.add("cf_acessou_resultado_de_colaborador");
		listFieldsCustom.add("cf_acessou_configuracoes_okr");
		listFieldsCustom.add("cf_criou_novo_objetivo_adm");
		listFieldsCustom.add("cf_criou_ciclo_demo_okr");
		listFieldsCustom.add("cf_criou_novo_ciclo_okr");
		listFieldsCustom.add("cf_acessou_lista_objetivos_adm");
		listFieldsCustom.add("cf_acessou_lista_objetivos");
		listFieldsCustom.add("cf_criou_novo_objetivo");
		listFieldsCustom.add("cf_criou_novo_kr");
		listFieldsCustom.add("cf_atualizou_progresso_kr");
		listFieldsCustom.add("cf_acessou_resultados");
		
		
		
		listFieldsCustom.add("cf_data_de_retorno_standby");
		listFieldsCustom.add("cf_jornada_do_lead");
		listFieldsCustom.add("cf_descarte_abrantes");
		listFieldsCustom.add("cf_o_lead_topou_bate_papo");
		listFieldsCustom.add("cf_email_pessoal");
		listFieldsCustom.add("cf_content");
		listFieldsCustom.add("cf_empresa_indicou");
		listFieldsCustom.add("cf_tipo_de_material");
		listFieldsCustom.add("cf_tipo_de_material_consumido");
		listFieldsCustom.add("cf_material_consumido");
		listFieldsCustom.add("cf_material_rico");
		listFieldsCustom.add("cf_visualizou_preview_relatorio");
		listFieldsCustom.add("cf_visualizou_planos_e_precos");
		listFieldsCustom.add("cf_visualizou_saldo_usuarios");
		listFieldsCustom.add("cf_solicitou_tempo_trial");
		listFieldsCustom.add("cf_engajado_teste");
		listFieldsCustom.add("cf_indicacao");
		listFieldsCustom.add("cf_desafio (company)");
		listFieldsCustom.add("cf_subsetor_de_ti (company)");
		listFieldsCustom.add("cf_status_do_descarte");
		listFieldsCustom.add("cf_setor");
		listFieldsCustom.add("cf_calculou_plano");
		listFieldsCustom.add("cf_qtd_conversoes_rd");
		listFieldsCustom.add("cf_lead_score_rd_perfil");
		listFieldsCustom.add("cf_lead_score_rd_interesse");
		listFieldsCustom.add("cf_origem_rd");
		listFieldsCustom.add("cf_conversoes_rd");
		//listFieldsCustom.add("cf_cadastro_manual_-_erro_integracao");
		
		
		
		
		
		listFieldsCustomContact.add("cf_calculou_creditos");
		listFieldsCustomContact.add("cf_comprou_creditos");
		listFieldsCustomContact.add("cf_converteu_site"); 
		listFieldsCustomContact.add("cf_converteu_trial"); 
		listFieldsCustomContact.add("cf_criou_avaliacao"); 
		listFieldsCustomContact.add("cf_demo_realizada"); 
		listFieldsCustomContact.add("cf_email"); 
		listFieldsCustomContact.add("cf_e-mail"); 
		listFieldsCustomContact.add("cf_engajado"); 
		listFieldsCustomContact.add("cf_enviou_avaliacao");
		listFieldsCustomContact.add("cf_numero_de_funcionarios"); 
		listFieldsCustomContact.add("cf_recebeu_invite"); 
		listFieldsCustomContact.add("cf_solicitou_consultor"); 
		listFieldsCustomContact.add("cf_solicitou_demo"); 
		listFieldsCustomContact.add("cf_solicitou_proposta"); 
		listFieldsCustomContact.add("cf_target_sales_campaign"); 
		listFieldsCustomContact.add("cf_visualizou_relatorio"); 
		
		
		listFieldsAccount.add("id");
		listFieldsAccount.add("last_contacted");
		listFieldsAccount.add("last_contacted_mode");
		listFieldsAccount.add("last_contacted_sales_activity_mode");
		listFieldsAccount.add("name");
		listFieldsAccount.add("number_of_employees");
		listFieldsAccount.add("open_deals_amount");
		listFieldsAccount.add("owner_id");
		listFieldsAccount.add("updated_at");
		listFieldsAccount.add("website");
		listFieldsAccount.add("industry_type_id");
		listFieldsAccount.add("business_type_id");
		listFieldsAccount.add("address");
		listFieldsAccount.add("city");
		listFieldsAccount.add("country");
		listFieldsAccount.add("phone");
		listFieldsAccount.add("state");
		listFieldsAccount.add("zipcode");
		listFieldsAccount.add("territory_id");
		
		listFieldsAccount.add("sales_account_cf_colaboradores");
		
		
		 listFieldsContact.add("active_sales_sequences"); 
		 listFieldsContact.add("address");
		 listFieldsContact.add("avatar");
		 listFieldsContact.add("city");
		 listFieldsContact.add("completed_sales_sequences");
		 listFieldsContact.add("country");
		 listFieldsContact.add("created_at");
		 listFieldsContact.add("creater_id");
		 listFieldsContact.add("department");
		 listFieldsContact.add("display_name");
		 listFieldsContact.add("do_not_disturb");
		 listFieldsContact.add("duplicates_searched_today");
		 listFieldsContact.add("email");
		 listFieldsContact.add("email_status");
		 listFieldsContact.add("facebook");
		 listFieldsContact.add("first_name");
		 listFieldsContact.add("has_authority");
		 listFieldsContact.add("has_connections");
		 listFieldsContact.add("has_duplicates");
		 listFieldsContact.add("id");
		 listFieldsContact.add("job_title");
		 listFieldsContact.add("keyword");
		 listFieldsContact.add("last_assigned_at");
		 listFieldsContact.add("last_contacted");
		 listFieldsContact.add("last_contacted_mode");
		 listFieldsContact.add("last_contacted_sales_activity_mode");
		 listFieldsContact.add("last_contacted_via_chat");
		 listFieldsContact.add("last_contacted_via_sales_activity");
		 listFieldsContact.add("last_name");
		 listFieldsContact.add("last_seen");
		 listFieldsContact.add("lead_quality");
		 listFieldsContact.add("lead_score");
		 listFieldsContact.add("linkedin");
		 listFieldsContact.add("medium");
		 listFieldsContact.add("mobile_number");
		 listFieldsContact.add("open_deals_amount");
		 listFieldsContact.add("open_deals_count");
		 listFieldsContact.add("recent_note");
		 listFieldsContact.add("state");
		 listFieldsContact.add("twitter");
		 listFieldsContact.add("updated_at");
		 listFieldsContact.add("updater_id");
		 listFieldsContact.add("web_form_ids");
		 listFieldsContact.add("won_deals_amount");
		 listFieldsContact.add("won_deals_count");
		 listFieldsContact.add("work_number");
		 listFieldsContact.add("zipcode");
		 
		 listFieldsContact.add("owner_id");
		
		 listFieldsContact.add("lead_source_id");
		 listFieldsContact.add("territory_id");
		 listFieldsContact.add("campaign_id");
		 
		 listFieldsContact.add("cf_cadastro_manual");
		 
		 
		 
		 listFieldsSales.add("end_date");
		 listFieldsSales.add("notes");
		 listFieldsSales.add("owner_id");
		 listFieldsSales.add("remote_id");
		 listFieldsSales.add("import_id");
		 listFieldsSales.add("latitude");
		 listFieldsSales.add("created_at");
		 listFieldsSales.add("title");
		 listFieldsSales.add("creater_id");
		 listFieldsSales.add("conversation_time");
		 listFieldsSales.add("updated_at");
		 listFieldsSales.add("updated_id");
		 listFieldsSales.add("targetable_type");
		 listFieldsSales.add("checkedin_at");
		 listFieldsSales.add("sales_activity_type_id");
		 listFieldsSales.add("sales_activity_outcome_id");
		 listFieldsSales.add("location");
		 listFieldsSales.add("id");
		 listFieldsSales.add("targetable_id");
		 listFieldsSales.add("start_date");
		 listFieldsSales.add("longitude");
		 
		 listFieldsLeadsActivites.add("composite_id");
		 listFieldsLeadsActivites.add("action_type");
		 listFieldsLeadsActivites.add("ac_created_at");
		 listFieldsLeadsActivites.add("user_activity");
		 listFieldsLeadsActivites.add("targetable_type");
		 listFieldsLeadsActivites.add("targetable_id");
		 listFieldsLeadsActivites.add("actionable_type");
		 listFieldsLeadsActivites.add("actionable_id");
		 listFieldsLeadsActivites.add("targetable_name");
		 listFieldsLeadsActivites.add("user_id");

	}
	
	
	
	
	/**
	 * 
	 * @param sql
	 * @param listValores
	 * @param id
	 */
	public void insereLead(String sql,List<Object> listValores) {
		 PreparedStatement pstm = null;
		 try {
			 
			 
			 String[] aVa = sql.split("!#!");
			 sql = aVa[0];
			
			 pstm = conexao.prepareStatement(sql);
			 for(int i = 0 ; i < listValores.size(); i++) {
				 if(listValores.get(i).equals("NULL")) {
					 String str = null;
					 pstm.setObject(i+1, str);
				 }else 
				    pstm.setObject(i+1, listValores.get(i));
			 }
			
			 pstm.executeUpdate();
				 
					
			}catch (Exception e) {
				System.out.println("erro : "+e.toString());
				e.printStackTrace();
				escreveLog("Erro ao inserir/update: "+sql+" - Erro : "+e.toString());
		
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	
	public void insereDeal(String sql,List<Object> listValores) {
		 PreparedStatement pstm = null;
		 try {
			 
			 String[] aVa = sql.split("!#!");
			 sql = aVa[0];
			
			 pstm = conexao.prepareStatement(sql);
			 for(int i = 0 ; i < listValores.size(); i++) {
				 if(listValores.get(i).equals("NULL")) {
					 String str = null;
					 pstm.setObject(i+1, str);
				 }else 
				    pstm.setObject(i+1, listValores.get(i));
			 }
			
			 pstm.executeUpdate();
				 
					
			}catch (Exception e) {
				System.out.println("erro ao inserir deal : "+sql);
				System.out.println("erro : "+e.toString());
				e.printStackTrace();
				escreveLog("Erro ao inserir deal : "+sql+"  - Erro : "+e.toString());
		
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	public void updateContato(String idContato, String idDeal, String name, String amount) {
		 PreparedStatement pstm = null;
		 String sql = "";
		 try {
			 
			 sql = "update freshsales_lead set deal_id = '"+idContato+"', "
				 		+ " deal_name = ? , deal_amount = ? where id = '"+idContato+"' ";
			
			 pstm = conexao.prepareStatement(sql);
			 pstm.setString(1, name);
			 pstm.setString(2, amount);
			 pstm.executeUpdate();
				 
					
			}catch (Exception e) {
				escreveLog("Erro ao atualizar contato "+sql+" - Erro : "+e.toString());
				System.out.println("Erro ao atualizar contato "+sql+" - Erro : "+e.toString());
		
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	public void updateDealContato(String idDeal, String active_sales_sequences,
			String address,
			String avatar,
			String city,
			String completed_sales_sequences,
			String country,
			String created_at,
			String creater_id,
			String department,
			String display_name,
			String do_not_disturb,
			String duplicates_searched_today,
			String email,
			String email_status,
			String facebook,
			String first_name,
			String has_authority,
			String has_connections,
			String has_duplicates,
			String id,
			String job_title,
			String keyword,
			String last_assigned_at,
			String last_contacted,
			String last_contacted_mode,
			String last_contacted_sales_activity_mode,
			String last_contacted_via_chat,
			String last_contacted_via_sales_activity,
			String last_name,
			String last_seen,
			String lead_quality,
			String lead_score,
			String linkedin,
			String medium,
			String mobile_number,
			String open_deals_amount,
			String open_deals_count,
			String recent_note,
			String state,
			String twitter,
			String updated_at,
			String updater_id,
			String web_form_ids,
			String won_deals_amount,
			String won_deals_count,
			String work_number,
			String zipcode,
			String cf_calculou_creditos,
			String cf_comprou_creditos,
			String cf_converteu_site,
			String cf_converteu_trial,
			String cf_criou_avaliacao,
			String cf_demo_realizada,
			String cf_email,
			String cf_email_alternative,
			String cf_engajado,
			String cf_enviou_avaliacao,
			String cf_numero_de_funcionarios,
			String cf_recebeu_invite,
			String cf_solicitou_consultor,
			String cf_solicitou_demo,
			String cf_solicitou_proposta,
			String cf_target_sales_campaign,
			String cf_visualizou_relatorio,
			String campaign_id,
			String campaign_name,
			String lead_source_id,
			String lead_source_name,
			String lead_source_partial,
			String lead_source_position,
			String territory_id,
			String territory_name,
			String territory_partial,
			String territory_position,
			String owner_display_name,
			String owner_email,
			String owner_mobile_number,
			String owner_is_active,
			String owner_work_number,
			String owner_id,
			String company_address,
			String company_annual_revenue,
			String company_business_type,
			String company_business_type_id,
			String company_city,
			String company_country,
			String company_id,
			String company_industry_type,
			String company_industry_type_id,
			String company_name,
			String company_number_of_employees,
			String company_phone,
			String company_state,
			String company_website,
			String company_zipcode) {
		 PreparedStatement pstm = null;
		 try {
			 
			
			 pstm = conexao.prepareStatement("update freshsales_deal set contact_active_sales_sequences = ?," + 
			 		"contact_address = ?," + 
			 		"contact_avatar = ?," + 
			 		"contact_city = ?," + 
			 		"contact_completed_sales_sequences = ?," + 
			 		"contact_country = ?," + 
			 		"contact_created_at = ?," + 
			 		"contact_creater_id = ?," + 
			 		"contact_department = ?," + 
			 		"contact_display_name = ?," + 
			 		"contact_do_not_disturb = ?," + 
			 		"contact_duplicates_searched_today = ?," + 
			 		"contact_email = ?," + 
			 		"contact_email_status = ?," + 
			 		"contact_facebook = ?," + 
			 		"contact_first_name = ?," + 
			 		"contact_has_authority = ?," + 
			 		"contact_has_connections = ?," + 
			 		"contact_has_duplicates = ?," + 
			 		"contact_id = ?," + 
			 		"contact_job_title = ?," + 
			 		"contact_keyword = ?," + 
			 		"contact_last_assigned_at = ?," + 
			 		"contact_last_contacted = ?," + 
			 		"contact_last_contacted_mode = ?," + 
			 		"contact_last_contacted_sales_activity_mode = ?," + 
			 		"contact_last_contacted_via_chat = ?," + 
			 		"contact_last_contacted_via_sales_activity = ?," + 
			 		"contact_last_name = ?," + 
			 		"contact_last_seen = ?," + 
			 		"contact_lead_quality = ?," + 
			 		"contact_lead_score = ?," + 
			 		"contact_linkedin = ?," + 
			 		"contact_medium = ?," + 
			 		"contact_mobile_number = ?," + 
			 		"contact_open_deals_amount = ?," + 
			 		"contact_open_deals_count = ?," + 
			 		"contact_recent_note = ?," + 
			 		"contact_state = ?," + 
			 		"contact_twitter = ?," + 
			 		"contact_updated_at = ?," + 
			 		"contact_updater_id = ?," + 
			 		"contact_web_form_ids = ?," + 
			 		"contact_won_deals_amount = ?," + 
			 		"contact_won_deals_count = ?," + 
			 		"contact_work_number = ?," + 
			 		"contact_zipcode = ?," + 
			 		"contact_cf_calculou_creditos = ?," + 
			 		"contact_cf_comprou_creditos = ?," + 
			 		"contact_cf_converteu_site = ?," + 
			 		"contact_cf_converteu_trial = ?," + 
			 		"contact_cf_criou_avaliacao = ?," + 
			 		"contact_cf_demo_realizada = ?," + 
			 		"contact_cf_email = ?," + 
			 		"contact_cf_email_alternative = ?," + 
			 		"contact_cf_engajado = ?," + 
			 		"contact_cf_enviou_avaliacao = ?," + 
			 		"contact_cf_numero_de_funcionarios = ?," + 
			 		"contact_cf_recebeu_invite = ?," + 
			 		"contact_cf_solicitou_consultor = ?," + 
			 		"contact_cf_solicitou_demo = ?," + 
			 		"contact_cf_solicitou_proposta = ?," + 
			 		"contact_cf_target_sales_campaign = ?," + 
			 		"contact_cf_visualizou_relatorio = ?," + 
			 		"contact_campaign_id = ?," + 
			 		"contact_campaign_name = ?," + 
			 		"contact_lead_source_id = ?," + 
			 		"contact_lead_source_name = ?," + 
			 		"contact_lead_source_partial = ?," + 
			 		"contact_lead_source_position = ?," + 
			 		"contact_territory_id = ?," + 
			 		"contact_territory_name = ?," + 
			 		"contact_territory_partial = ?," + 
			 		"contact_territory_position = ?," + 
			 		"contact_owner_display_name = ?," + 
			 		"contact_owner_email = ?," + 
			 		"contact_owner_mobile_number = ?," + 
			 		"contact_owner_is_active = ?," + 
			 		"contact_owner_work_number = ?," + 
			 		"contact_owner_id = ?," + 
			 		"contact_company_address = ?," + 
			 		"contact_company_annual_revenue = ?," + 
			 		"contact_company_business_type = ?," + 
			 		"contact_company_business_type_id = ?," + 
			 		"contact_company_city = ?," + 
			 		"contact_company_country = ?," + 
			 		"contact_company_id = ?," + 
			 		"contact_company_industry_type = ?," + 
			 		"contact_company_industry_type_id = ?," + 
			 		"contact_company_name = ?," + 
			 		"contact_company_number_of_employees = ?," + 
			 		"contact_company_phone = ?," + 
			 		"contact_company_state = ?," + 
			 		"contact_company_website = ?," + 
			 		"contact_company_zipcode = ? where id = '"+idDeal+"' ");
			 
			 pstm.setString(1, active_sales_sequences);
			 pstm.setString(2, address);
			 pstm.setString(3, avatar);
			 pstm.setString(4, city);
			 pstm.setString(5, completed_sales_sequences);
			 pstm.setString(6, country);
			 pstm.setString(7, created_at);
			 pstm.setString(8, creater_id);
			 pstm.setString(9, department);
			 pstm.setString(10, display_name);
			 pstm.setString(11, do_not_disturb);
			 pstm.setString(12, duplicates_searched_today);
			 pstm.setString(13, email);
			 pstm.setString(14, email_status);
			 pstm.setString(15, facebook);
			 pstm.setString(16, first_name);
			 pstm.setString(17, has_authority);
			 pstm.setString(18, has_connections);
			 pstm.setString(19, has_duplicates);
			 pstm.setString(20, id);
			 pstm.setString(21, job_title);
			 pstm.setString(22, keyword);
			 pstm.setString(23, last_assigned_at);
			 pstm.setString(24, last_contacted);
			 pstm.setString(25, last_contacted_mode);
			 pstm.setString(26, last_contacted_sales_activity_mode);
			 pstm.setString(27, last_contacted_via_chat);
			 pstm.setString(28, last_contacted_via_sales_activity);
			 pstm.setString(29, last_name);
			 pstm.setString(30, last_seen);
			 pstm.setString(31, lead_quality);
			 pstm.setString(32, lead_score);
			 pstm.setString(33, linkedin);
			 pstm.setString(34, medium);
			 pstm.setString(35, mobile_number);
			 pstm.setString(36, open_deals_amount);
			 pstm.setString(37, open_deals_count);
			 pstm.setString(38, recent_note);
			 pstm.setString(39, state);
			 pstm.setString(40, twitter);
			 pstm.setString(41, updated_at);
			 pstm.setString(42, updater_id);
			 pstm.setString(43, web_form_ids);
			 pstm.setString(44, won_deals_amount);
			 pstm.setString(45, won_deals_count);
			 pstm.setString(46, work_number);
			 pstm.setString(47, zipcode);
			 pstm.setString(48, cf_calculou_creditos);
			 pstm.setString(49, cf_comprou_creditos);
			 pstm.setString(50, cf_converteu_site);
			 pstm.setString(51, cf_converteu_trial);
			 pstm.setString(52, cf_criou_avaliacao);
			 pstm.setString(53, cf_demo_realizada);
			 pstm.setString(54, cf_email);
			 pstm.setString(55, cf_email_alternative);
			 pstm.setString(56, cf_engajado);
			 pstm.setString(57, cf_enviou_avaliacao);
			 pstm.setString(58, cf_numero_de_funcionarios);
			 pstm.setString(59, cf_recebeu_invite);
			 pstm.setString(60, cf_solicitou_consultor);
			 pstm.setString(61, cf_solicitou_demo);
			 pstm.setString(62, cf_solicitou_proposta);
			 pstm.setString(63, cf_target_sales_campaign);
			 pstm.setString(64, cf_visualizou_relatorio);
			 pstm.setString(65, campaign_id);
			 pstm.setString(66, campaign_name);
			 pstm.setString(67, lead_source_id);
			 pstm.setString(68, lead_source_name);
			 pstm.setString(69, lead_source_partial);
			 pstm.setString(70, lead_source_position);
			 pstm.setString(71, territory_id);
			 pstm.setString(72, territory_name);
			 pstm.setString(73, territory_partial);
			 pstm.setString(74, territory_position);
			 pstm.setString(75, owner_display_name);
			 pstm.setString(76, owner_email);
			 pstm.setString(77, owner_mobile_number);
			 pstm.setString(78, owner_is_active);
			 pstm.setString(79, owner_work_number);
			 pstm.setString(80, owner_id);
			 pstm.setString(81, company_address);
			 pstm.setString(82, company_annual_revenue);
			 pstm.setString(83, company_business_type);
			 pstm.setString(84, company_business_type_id);
			 pstm.setString(85, company_city);
			 pstm.setString(86, company_country);
			 pstm.setString(87, company_id);
			 pstm.setString(88, company_industry_type);
			 pstm.setString(89, company_industry_type_id);
			 pstm.setString(90, company_name);
			 pstm.setString(91, company_number_of_employees);
			 pstm.setString(92, company_phone);
			 pstm.setString(93, company_state);
			 pstm.setString(94, company_website);
			 pstm.setString(95, company_zipcode);
			 
			 pstm.executeUpdate();
				 
					
			}catch (Exception e) {
				//escreveLog("Erro ao atualizar contato - Erro : "+e.toString());
		
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	public List<String> carregaLeadNull(List<String> listIds) {
		 PreparedStatement pstm = null;
		 ResultSet rs = null;
		 List<String> list = new ArrayList<String>();
		 try {
			 
			
			 pstm = conexao.prepareStatement(" SELECT lead_id FROM freshsales_leads_activities WHERE ac_created_at is NULL ");
			 
			 rs = pstm.executeQuery();
			 while(rs.next()){
				 String id = rs.getString("lead_id");
				 if(!listIds.contains(id)) {
					 list.add(id); 
				 }
			 }
			 
		 }catch (Exception e) {
				//escreveLog("Erro ao carregando contato antigo - Erro : "+e.toString());
		
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					if(rs != null) rs.close();
					rs = null;
					
				} catch (SQLException e) {}	
			}
		 
		 return list;
	}
			 
	
	
	public void carregaContatoAntigo(String ids, String idDeal) {
		 PreparedStatement pstm = null;
		 ResultSet rs = null;
		 try {
			 
			
			 pstm = conexao.prepareStatement(" SELECT *  FROM freshsales_lead "
			 		+ " WHERE id IN ("+ids+") ORDER BY created_at ASC LIMIT 1 ");
			 
			 rs = pstm.executeQuery();
			 while(rs.next()){
				 String active_sales_sequences = rs.getString("active_sales_sequences");
				 String address = rs.getString("address");
				 String avatar = rs.getString("avatar");
				 String city = rs.getString("city");
				 String completed_sales_sequences = rs.getString("completed_sales_sequences");
				 String country = rs.getString("country");
				 String created_at = rs.getString("created_at");
				 String creater_id = rs.getString("creater_id");
				 String department = rs.getString("department");
				 String display_name = rs.getString("display_name");
				 String do_not_disturb = rs.getString("do_not_disturb");
				 String duplicates_searched_today = rs.getString("duplicates_searched_today");
				 String email = rs.getString("email");
				 String email_status = rs.getString("email_status");
				 String facebook = rs.getString("facebook");
				 String first_name = rs.getString("first_name");
				 String has_authority = rs.getString("has_authority");
				 String has_connections = rs.getString("has_connections");
				 String has_duplicates = rs.getString("has_duplicates");
				 String id = rs.getString("id");
				 String job_title = rs.getString("job_title");
				 String keyword = rs.getString("keyword");
				 String last_assigned_at = rs.getString("last_assigned_at");
				 String last_contacted = rs.getString("last_contacted");
				 String last_contacted_mode = rs.getString("last_contacted_mode");
				 String last_contacted_sales_activity_mode = rs.getString("last_contacted_sales_activity_mode");
				 String last_contacted_via_chat = rs.getString("last_contacted_via_chat");
				 String last_contacted_via_sales_activity = rs.getString("last_contacted_via_sales_activity");
				 String last_name = rs.getString("last_name");
				 String last_seen = rs.getString("last_seen");
				 String lead_quality = rs.getString("lead_quality");
				 String lead_score = rs.getString("lead_score");
				 String linkedin = rs.getString("linkedin");
				 String medium = rs.getString("medium");
				 String mobile_number = rs.getString("mobile_number");
				 String open_deals_amount = rs.getString("open_deals_amount");
				 String open_deals_count = rs.getString("open_deals_count");
				 String recent_note = rs.getString("recent_note");
				 String state = rs.getString("state");
				 String twitter = rs.getString("twitter");
				 String updated_at = rs.getString("updated_at");
				 String updater_id = rs.getString("updater_id");
				 String web_form_ids = rs.getString("web_form_ids");
				 String won_deals_amount = rs.getString("won_deals_amount");
				 String won_deals_count = rs.getString("won_deals_count");
				 String work_number = rs.getString("work_number");
				 String zipcode = rs.getString("zipcode");
				 String cf_calculou_creditos = rs.getString("cf_calculou_creditos");
				 String cf_comprou_creditos = rs.getString("cf_comprou_creditos");
				 String cf_converteu_site = rs.getString("cf_converteu_site");
				 String cf_converteu_trial = rs.getString("cf_converteu_trial");
				 String cf_criou_avaliacao = rs.getString("cf_criou_avaliacao");
				 String cf_demo_realizada = rs.getString("cf_demo_realizada");
				 String cf_email = rs.getString("cf_email");
				 String cf_email_alternative = rs.getString("cf_email_alternative");
				 String cf_engajado = rs.getString("cf_engajado");
				 String cf_enviou_avaliacao = rs.getString("cf_enviou_avaliacao");
				 String cf_numero_de_funcionarios = rs.getString("cf_numero_de_funcionarios");
				 String cf_recebeu_invite = rs.getString("cf_recebeu_invite");
				 String cf_solicitou_consultor = rs.getString("cf_solicitou_consultor");
				 String cf_solicitou_demo = rs.getString("cf_solicitou_demo");
				 String cf_solicitou_proposta = rs.getString("cf_solicitou_proposta");
				 String cf_target_sales_campaign = rs.getString("cf_target_sales_campaign");
				 String cf_visualizou_relatorio = rs.getString("cf_visualizou_relatorio");
				 String campaign_id = rs.getString("campaign_id");
				 String campaign_name = rs.getString("campaign_name");
				 String lead_source_id = rs.getString("lead_source_id");
				 String lead_source_name = rs.getString("lead_source_name");
				 String lead_source_partial = rs.getString("lead_source_partial");
				 String lead_source_position = rs.getString("lead_source_position");
				 String territory_id = rs.getString("territory_id");
				 String territory_name = rs.getString("territory_name");
				 String territory_partial = rs.getString("territory_partial");
				 String territory_position = rs.getString("territory_position");
				 String owner_display_name = rs.getString("owner_display_name");
				 String owner_email = rs.getString("owner_email");
				 String owner_mobile_number = rs.getString("owner_mobile_number");
				 String owner_is_active = rs.getString("owner_is_active");
				 String owner_work_number = rs.getString("owner_work_number");
				 String owner_id = rs.getString("owner_id");
				 String company_address = rs.getString("company_address");
				 String company_annual_revenue = rs.getString("company_annual_revenue");
				 String company_business_type = rs.getString("company_business_type");
				 String company_business_type_id = rs.getString("company_business_type_id");
				 String company_city = rs.getString("company_city");
				 String company_country = rs.getString("company_country");
				 String company_id = rs.getString("company_id");
				 String company_industry_type = rs.getString("company_industry_type");
				 String company_industry_type_id = rs.getString("company_industry_type_id");
				 String company_name = rs.getString("company_name");
				 String company_number_of_employees = rs.getString("company_number_of_employees");
				 String company_phone = rs.getString("company_phone");
				 String company_state = rs.getString("company_state");
				 String company_website = rs.getString("company_website");
				 String company_zipcode = rs.getString("company_zipcode");
				 
				 this.updateDealContato(idDeal,active_sales_sequences,
						 address,
						 avatar,
						 city,
						 completed_sales_sequences,
						 country,
						 created_at,
						 creater_id,
						 department,
						 display_name,
						 do_not_disturb,
						 duplicates_searched_today,
						 email,
						 email_status,
						 facebook,
						 first_name,
						 has_authority,
						 has_connections,
						 has_duplicates,
						 id,
						 job_title,
						 keyword,
						 last_assigned_at,
						 last_contacted,
						 last_contacted_mode,
						 last_contacted_sales_activity_mode,
						 last_contacted_via_chat,
						 last_contacted_via_sales_activity,
						 last_name,
						 last_seen,
						 lead_quality,
						 lead_score,
						 linkedin,
						 medium,
						 mobile_number,
						 open_deals_amount,
						 open_deals_count,
						 recent_note,
						 state,
						 twitter,
						 updated_at,
						 updater_id,
						 web_form_ids,
						 won_deals_amount,
						 won_deals_count,
						 work_number,
						 zipcode,
						 cf_calculou_creditos,
						 cf_comprou_creditos,
						 cf_converteu_site,
						 cf_converteu_trial,
						 cf_criou_avaliacao,
						 cf_demo_realizada,
						 cf_email,
						 cf_email_alternative,
						 cf_engajado,
						 cf_enviou_avaliacao,
						 cf_numero_de_funcionarios,
						 cf_recebeu_invite,
						 cf_solicitou_consultor,
						 cf_solicitou_demo,
						 cf_solicitou_proposta,
						 cf_target_sales_campaign,
						 cf_visualizou_relatorio,
						 campaign_id,
						 campaign_name,
						 lead_source_id,
						 lead_source_name,
						 lead_source_partial,
						 lead_source_position,
						 territory_id,
						 territory_name,
						 territory_partial,
						 territory_position,
						 owner_display_name,
						 owner_email,
						 owner_mobile_number,
						 owner_is_active,
						 owner_work_number,
						 owner_id,
						 company_address,
						 company_annual_revenue,
						 company_business_type,
						 company_business_type_id,
						 company_city,
						 company_country,
						 company_id,
						 company_industry_type,
						 company_industry_type_id,
						 company_name,
						 company_number_of_employees,
						 company_phone,
						 company_state,
						 company_website,
						 company_zipcode);
				 
			 }
					
			}catch (Exception e) {
				escreveLog("Erro ao carregando contato antigo - Erro : "+e.toString());
				System.out.println("Erro ao carregando contato antigo - Erro : "+e.toString());
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					if(rs != null) rs.close();
					rs = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	/**
	 * 
	 * @param sql
	 * @param listValores
	 * @param id
	 */
	public void insereContato(String sql,List<Object> listValores) {
		 PreparedStatement pstm = null;
		 try {
			 
			 String[] aVa = sql.split("!#!");
			 sql = aVa[0];
			
			 pstm = conexao.prepareStatement(sql);
			 for(int i = 0 ; i < listValores.size(); i++) {
				 if(listValores.get(i).equals("NULL")) {
					 String str = null;
					 pstm.setObject(i+1, str);
				 }else 
				    pstm.setObject(i+1, listValores.get(i));
			 }
			
			 pstm.executeUpdate();
				 
					
			}catch (Exception e) {
				System.out.println("Erro insert : "+sql);
				System.out.println("Erro ao inserir o contato : "+e.toString());
				e.printStackTrace();
		
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	
	public void insereSales(String sql,List<Object> listValores) {
		 PreparedStatement pstm = null;
		 try {
			 
			 String[] aVa = sql.split("!#!");
			 sql = aVa[0];
			
			 pstm = conexao.prepareStatement(sql);
			 for(int i = 0 ; i < listValores.size(); i++) {
				 if(listValores.get(i).equals("NULL")) {
					 String str = null;
					 pstm.setObject(i+1, str);
				 }else 
				    pstm.setObject(i+1, listValores.get(i));
			 }
			
			 pstm.executeUpdate();
				 
					
			}catch (Exception e) {
				//escreveLog("Erro ao inserir: "+sql+" - Erro : "+e.toString());
		
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	/**
	 * Remove os registro da tabela
	 */
	public void deletelead() {
		 PreparedStatement pstm = null;
		 try {
			
			 pstm = conexao.prepareStatement("delete from freshsales_lead where type_name = 'lead' ");
			 pstm.executeUpdate();
					
			}catch (Exception e) {
				//escreveLog("Erro delete lead "+e.toString());
				
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	
	public void deleteactivites(String id) {
		 PreparedStatement pstm = null;
		 try {
			
			 pstm = conexao.prepareStatement("delete from freshsales_leads_activities where lead_id = '"+id+"' ");
			 pstm.executeUpdate();
					
			}catch (Exception e) {
				//escreveLog("Erro delete freshsales_leads_activities "+e.toString());
				
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	
	public void deleteconfiguration(String tabela) {
		 PreparedStatement pstm = null;
		 try {
			
			 pstm = conexao.prepareStatement("delete from "+tabela);
			 pstm.executeUpdate();
					
			}catch (Exception e) {
				//escreveLog("Erro delete "+tabela+" "+e.toString());
				
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	public void deletecontact() {
		 PreparedStatement pstm = null;
		 try {
			
			 pstm = conexao.prepareStatement("delete from freshsales_lead where type_name = 'contact' ");
			 pstm.executeUpdate();
					
			}catch (Exception e) {
				//escreveLog("Erro delete lead "+e.toString());
				
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	
	public void deletesales(String token) {
		 PreparedStatement pstm = null;
		 try {
			
			 pstm = conexao.prepareStatement("delete from freshsales_sales_activities where token = '"+token+"' ");
			 pstm.executeUpdate();
					
			}catch (Exception e) {
				//escreveLog("Erro delete sales "+e.toString());
				
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	
	public void deletedeal() {
		 PreparedStatement pstm = null;
		 try {
			
			 pstm = conexao.prepareStatement("delete from freshsales_deal");
			 pstm.executeUpdate();
					
			}catch (Exception e) {
			//	escreveLog("Erro delete deal "+e.toString());
				
			}finally{
				try {
					if(pstm != null) pstm.close();
					pstm = null;
					
				} catch (SQLException e) {}	
			}
		 
		   
	}
	

	
	/**
	 * Vai buscar todos os ids e retornar um array com todos os ids
	 * @return
	 */
	public HashMap<String, List<Object>> getAllLeads(List<String> listIds){
		  HashMap<String, List<Object>> listLeads = new HashMap<String, List<Object>>();
		  HashMap<String, List<Object>> listIdsLeads = new HashMap<String, List<Object>>();
		  int page = 1;
		  HashMap<String, List<Object>> list = new HashMap<String, List<Object>>();
		  
		  listLeads = listAllLeads(page,  listIds, list);
		  while(listLeads.size() > 0) {
			  Iterator<String> it = listLeads.keySet().iterator();
			  while(it.hasNext()) {
				  String key = it.next();
				  listIdsLeads.put(key, listLeads.get(key));
			  }
			  listLeads = new HashMap<String, List<Object>>();
			  list = new HashMap<String, List<Object>>();
			  page++;
			  listLeads = listAllLeads(page,   listIds, list);
		  }
		  return listIdsLeads;
	}
	
	
	public HashMap<String, List<Object>> getAllDeals1(HashMap<String, SalesAccount> listAllAccounts){
		  HashMap<String, List<Object>> listLeads = new HashMap<String, List<Object>>();
		  HashMap<String, List<Object>> listIdsLeads = new HashMap<String, List<Object>>();
		  int page = 1;
		  HashMap<String, List<Object>> list = new HashMap<String, List<Object>>();
		  listLeads = listAllDeals1(page, listAllAccounts, list);
		  while(listLeads.size() > 0) {
			  Iterator<String> it = listLeads.keySet().iterator();
			  while(it.hasNext()) {
				  String key = it.next();
				  listIdsLeads.put(key, listLeads.get(key));
			  }
			  listLeads = new HashMap<String, List<Object>>();
			  page++;
			  list = new HashMap<String, List<Object>>();
			  listLeads = listAllDeals1(page, listAllAccounts, list);
		  }
		  return listIdsLeads;
	}
	
	
	public HashMap<String, List<Object>> getAllDeals2(HashMap<String, SalesAccount> listAllAccounts){
		  HashMap<String, List<Object>> listLeads = new HashMap<String, List<Object>>();
		  HashMap<String, List<Object>> listIdsLeads = new HashMap<String, List<Object>>();
		  int page = 1;
		  HashMap<String, List<Object>> list = new HashMap<String, List<Object>>();
		  listLeads = listAllDeals2(page, listAllAccounts, list);
		  while(listLeads.size() > 0) {
			  Iterator<String> it = listLeads.keySet().iterator();
			  while(it.hasNext()) {
				  String key = it.next();
				  listIdsLeads.put(key, listLeads.get(key));
			  }
			  listLeads = new HashMap<String, List<Object>>();
			  page++;
			  list = new HashMap<String, List<Object>>();
			  listLeads = listAllDeals2(page, listAllAccounts, list);
		  }
		  return listIdsLeads;
	}
	
	
	public HashMap<String, SalesAccount> getAllAccounts(){
		  HashMap<String, SalesAccount> listLeads = new HashMap<String, SalesAccount>();
		  HashMap<String, SalesAccount> listIdsLeads = new HashMap<String, SalesAccount>();
		  int page = 1;
			HashMap<String, SalesAccount> list = new HashMap<String, SalesAccount>();
		  listLeads = listAllAccounts(page, list);
		  while(listLeads.size() > 0) {
			  Iterator<String> it = listLeads.keySet().iterator();
			  while(it.hasNext()) {
				  String key = it.next();
				  listIdsLeads.put(key, listLeads.get(key));
			  }
			  listLeads = new HashMap<String, SalesAccount>();
			  page++;
			  list = new HashMap<String, SalesAccount>();
			  listLeads = listAllAccounts(page, list);
		  }
		  return listIdsLeads;
	}
	
	
	public HashMap<String, List<Object>> getAllContacts(){
		HashMap<String, List<Object>> listLeads = new HashMap<String, List<Object>>();
		HashMap<String, List<Object>> listIdsLeads = new HashMap<String, List<Object>>();
		  int page = 1;
		  HashMap<String, List<Object>> list = new HashMap<String, List<Object>>();
		  listLeads = listAllContact(page, list);
		  while(listLeads.size() > 0) {
			  Iterator<String> it = listLeads.keySet().iterator();
			  while(it.hasNext()) {
				  String key = it.next();
				  listIdsLeads.put(key, listLeads.get(key));
			  }
			  listLeads = new HashMap<String, List<Object>>();
			  list = new HashMap<String, List<Object>>();
			  page++;
			  listLeads = listAllContact(page, list);
		  }
		  return listIdsLeads;
	}
	
	
	public HashMap<String, List<Object>> getAllSales(String token, String owner_display_name, String owner_email){
		HashMap<String, List<Object>> listLeads = new HashMap<String, List<Object>>();
		HashMap<String, List<Object>> listIdsLeads = new HashMap<String, List<Object>>();
		  int page = 1;
		  listLeads = listAllSales(page, token, owner_display_name, owner_email);
		  while(listLeads.size() > 0) {
			  Iterator<String> it = listLeads.keySet().iterator();
			  while(it.hasNext()) {
				  String key = it.next();
				  listIdsLeads.put(key, listLeads.get(key));
			  }
			  listLeads = new HashMap<String, List<Object>>();
			  page++;
			  listLeads = listAllSales(page, token, owner_display_name, owner_email);
		  }
		  return listIdsLeads;
	}
	
	
	public HashMap<String, List<Object>> getAllActivites(String id){
		HashMap<String, List<Object>> listLeads = new HashMap<String, List<Object>>();
		HashMap<String, List<Object>> listIdsLeads = new HashMap<String, List<Object>>();
		  int page = 1;
		  listLeads = listAllActivites(page, id);
		  while(listLeads.size() > 0) {
			  Iterator<String> it = listLeads.keySet().iterator();
			  while(it.hasNext()) {
				  String key = it.next();
				  listIdsLeads.put(key, listLeads.get(key));
			  }
			  listLeads = new HashMap<String, List<Object>>();
			  page++;
			  listLeads = listAllActivites(page, id);
		  }
		  return listIdsLeads;
	}
	
	
	
	public HashMap<String, List<Object>> listAllSales(int page, String token, String owner_display_name, String owner_email) {
		HashMap<String, List<Object>> list = new HashMap<String, List<Object>>();
		JSONParser parser = new JSONParser();
		try {	
			String u = "https://impulse.freshsales.io/api/sales_activities";
         	URL url = new URL(u);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Token token="+token);

			if (conn.getResponseCode() == 200) {
			
              BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
              String output;
              StringBuffer strBuffer = new StringBuffer();
			  while ((output = br.readLine()) != null) {
				  strBuffer.append(output);
			  }
			  Object obj =  parser.parse(strBuffer.toString());
			  JSONObject jsonObject = (JSONObject) obj;
			  String colunasInsert = "";
			  String  valoresInsert = "";
			  String virgula = "";
			  List<Object>  listValores = new ArrayList<Object>();
			
			  JSONArray jsonObjectLeads = (JSONArray) jsonObject.get("sales_activities");
			  for(int j = 0 ; j < jsonObjectLeads.size(); j++) {
				  
				   colunasInsert = "";
				   valoresInsert = "";
				   virgula = "";
				   listValores = new ArrayList<Object>();
				  
				  JSONObject jsonObjectLead = (JSONObject)jsonObjectLeads.get(j);
				  
				  for(int i = 0; i < listFieldsSales.size(); i++) {
					  String coluna = listFieldsSales.get(i);
					  coluna = coluna.trim();
					  
					  try {
						  Object value = (Object)jsonObjectLead.get(coluna);
					      colunasInsert = colunasInsert + virgula + coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      if(value != null)
					         listValores.add(value.toString());
					      else {
					    	  value = "null";
					    	 // if(coluna.equalsIgnoreCase(""))
					    	  
					    	  listValores.add(value);
					      }
					      
					   
					      
					      
					  }catch (Exception e) {}
					  
				  }
				 
				  
				  
				  
				 
				  String insert = "insert into freshsales_sales_activities ("+colunasInsert+", owner_display_name, owner_email, token) values ("+valoresInsert+", '"+owner_display_name+"', '"+owner_email+"', '"+token+"')!#!"+UUID.randomUUID();
					
				  List<Object> listAux = new ArrayList<Object>();
					 for(int i = 0 ; i < listValores.size(); i++) {
						 String str = listValores.get(i).toString();
						 if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-03:00") != -1) {
							 str = str.replaceAll("T", " ");
							 str = str.replaceAll("-03:00", "");
							 str = str.trim();
							 listAux.add(str); 
						 }else if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-02:00") != -1) {
							 str = str.replaceAll("T", " ");
							 str = str.replaceAll("-02:00", "");
							 str = str.trim();
							 listAux.add(str); 
						 }else {
							 listAux.add(str); 
						 }
					 }
					 listValores = new ArrayList<Object>();
					 for(int i = 0 ; i < listAux.size(); i++) {
						 listValores.add(listAux.get(i)); 
					 }
				  
				  list.put(insert, listValores);
				  //insereContato(insert, listValores, id);
				  
				  
			  }
			  
			  
			  conn.disconnect();
			}
			 
			else if (conn.getResponseCode() == 429) {
					conn.disconnect();
					// esse status eh quando atigiu o limite de requisicao
					escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
					Thread.sleep(60000*30);
					//list = new ArrayList<String>();
					//listAllContact(page); 
	     }else {
	    	 escreveLog("Status listAllSales : "+conn.getResponseCode());
	     }
		  } catch (Exception e) {escreveLog("Erro ao buscar listAllSales : "+e.toString());} 
		
		 return list;
		
	}
	
	
	/**
	 * 
	 * @param page
	 * @param id
	 * @return
	 */
	public HashMap<String, List<Object>> listAllActivites(int page, String id) {
		HashMap<String, List<Object>> list = new HashMap<String, List<Object>>();
		JSONParser parser = new JSONParser();
		try {	
			
			
			String u = "https://impulse.freshsales.io/api/leads/"+id+"/activities?include=user&page="+page;
         	URL url = new URL(u);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Token token=wPBMcHQSK9yfCi3omDVGUQ");

			if (conn.getResponseCode() == 200) {
			
              BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
              String output;
              StringBuffer strBuffer = new StringBuffer();
			  while ((output = br.readLine()) != null) {
				  strBuffer.append(output);
			  }
			 // System.out.println(strBuffer.toString());
			  Object obj =  parser.parse(strBuffer.toString());
			  JSONObject jsonObject = (JSONObject) obj;
			  String colunasInsert = "";
			  String  valoresInsert = "";
			  String virgula = "";
			  List<Object>  listValores = new ArrayList<Object>();
			
			  JSONArray jsonObjectLeads = (JSONArray) jsonObject.get("activities");
			  for(int j = 0 ; j < jsonObjectLeads.size(); j++) {
				  
				   colunasInsert = "";
				   valoresInsert = "";
				   virgula = "";
				   listValores = new ArrayList<Object>();
				  
				  JSONObject jsonObjectLead = (JSONObject)jsonObjectLeads.get(j);
				  
				  for(int i = 0; i < listFieldsLeadsActivites.size(); i++) {
					  String coluna = listFieldsLeadsActivites.get(i);
					  coluna = coluna.trim();
					  
					  try {
						  if(coluna.equalsIgnoreCase("ac_created_at")) {
							  coluna = "created_at";
						  }
						  
						  Object value = (Object)jsonObjectLead.get(coluna);
						  
						  if(coluna.equalsIgnoreCase("created_at")) {
							  coluna = "ac_created_at";
						  }
						  
					      colunasInsert = colunasInsert + virgula + coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      if(value != null)
					         listValores.add(value.toString());
					      else {
					    	  value = "null";
					    	  if(coluna.equalsIgnoreCase("ac_created_at")) 
					    	       value = "NULL"; 
					    	  
					    	  listValores.add(value);
					      }
					      
					   
					      
					      
					  }catch (Exception e) {}
					  
				  }
				  
				  
				  
				  try {
					  JSONObject jsonObjectAction = (JSONObject)jsonObjectLead.get("action_data"); 
					  
					  
					  List<String> listAc = new ArrayList<String>();
					  listAc.add("email_id");
					  listAc.add("subject");
					  listAc.add("name");
					  listAc.add("count");
					  listAc.add("first_occurrence");
					  listAc.add("last_occurrence");
					  listAc.add("property_key");
					  listAc.add("property_value");
					  listAc.add("description");
					  listAc.add("end_date");
					  listAc.add("from_date");
					  listAc.add("time_zone");
					  listAc.add("title");
					  listAc.add("stage_id");
					  listAc.add("stage_name");
					  listAc.add("recording_url");
					  listAc.add("recording_duration");
					  listAc.add("outcome");
					  
					  for(int l=0;l<listAc.size();l++) {
						  String key = listAc.get(l);
						  
						  try { Object value = (Object)jsonObjectAction.get(key);
						  
						  colunasInsert = colunasInsert + virgula + "ac_data_"+key;
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      if(value != null)
					         listValores.add(value.toString());
					      else {
					    	  value = "null";
					    	  if(key.equalsIgnoreCase("first_occurrence")) {
					    		  value = "NULL"; 
					    	  }
					    	  if(key.equalsIgnoreCase("last_occurrence")) {
					    		  value = "NULL"; 
					    	  }
					    	  
					    	  listValores.add(value);
					      }
						         
						  }catch (Exception e) {}
						  
						  
						 
						  
					  }
					  
					  try {
						  JSONObject jsonObjectActionNumber = (JSONObject)jsonObjectAction.get("phone_caller"); 
						  
						  
						  Object value = (Object)jsonObjectActionNumber.get("number");
						  
						  colunasInsert = colunasInsert + virgula + " ac_data_phone_caller_number";
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      if(value != null)
					         listValores.add(value.toString());
					      else {
					    	  value = "null";
					    	  listValores.add(value);
					      }
						         
						  }catch (Exception e) {}
					  
					  
					 
										  
					  
				  }catch (Exception e) {
					
					  try {
						  Object value = (Object)jsonObjectLead.get("action_data");
					      colunasInsert = colunasInsert + virgula + "action_data";
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      if(value != null)
					         listValores.add(value.toString());
					      else {
					    	  value = "null";
					    	  listValores.add(value);
					      }
					      
					   
					      
					      
					  }catch (Exception ex) {}
					  
				}
				 
				  
				  
				  
				 
				  String insert = "insert into freshsales_leads_activities ("+colunasInsert+", lead_id) values ("+valoresInsert+", '"+id+"')!#!"+UUID.randomUUID();
					
				  List<Object> listAux = new ArrayList<Object>();
					 for(int i = 0 ; i < listValores.size(); i++) {
						 String str = listValores.get(i).toString();
						 if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-03:00") != -1) {
							 str = str.replaceAll("T", " ");
							 str = str.replaceAll("-03:00", "");
							 str = str.trim();
							 listAux.add(str); 
						 }else if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-02:00") != -1) {
							 str = str.replaceAll("T", " ");
							 str = str.replaceAll("-02:00", "");
							 str = str.trim();
							 listAux.add(str); 
						 }else {
							 listAux.add(str); 
						 }
					 }
					 listValores = new ArrayList<Object>();
					 for(int i = 0 ; i < listAux.size(); i++) {
						 listValores.add(listAux.get(i)); 
					 }
				  
				  
				  list.put(insert, listValores);
				  //insereContato(insert, listValores, id);
				  
				  
			  }
			  
			  
			  conn.disconnect();
			}
			 
			else if (conn.getResponseCode() == 429) {
					conn.disconnect();
					// esse status eh quando atigiu o limite de requisicao
					escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
					Thread.sleep(60000*30);
					//list = new ArrayList<String>();
					listAllActivites(page, id); 
	     }else {
	    	 conn.disconnect();
				// esse status eh quando atigiu o limite de requisicao
				escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
				Thread.sleep(60000*30);
				//list = new ArrayList<String>();
				listAllActivites(page, id); 
	     }
		  } catch (Exception e) {escreveLog("Erro ao buscar listAllActivites : "+e.toString());} 
		
		 return list;
		
	}
	
	
	
	public HashMap<String, List<Object>> listConfiguration(String method, List<String> listCampos, String tabela) {
		HashMap<String, List<Object>> list = new HashMap<String, List<Object>>();
		JSONParser parser = new JSONParser();
		try {	
			String u = "https://impulse.freshsales.io/api/selector/"+method;
         	URL url = new URL(u);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Token token=wPBMcHQSK9yfCi3omDVGUQ");

			if (conn.getResponseCode() == 200) {
			
              BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
              String output;
              StringBuffer strBuffer = new StringBuffer();
			  while ((output = br.readLine()) != null) {
				  strBuffer.append(output);
			  }
			  Object obj =  parser.parse(strBuffer.toString());
			  JSONObject jsonObject = (JSONObject) obj;
			  String colunasInsert = "";
			  String  valoresInsert = "";
			  String virgula = "";
			  List<Object>  listValores = new ArrayList<Object>();
			
			  JSONArray jsonObjectLeads = (JSONArray) jsonObject.get(method);
			  for(int j = 0 ; j < jsonObjectLeads.size(); j++) {
				  
				   colunasInsert = "";
				   valoresInsert = "";
				   virgula = "";
				   listValores = new ArrayList<Object>();
				  
				  JSONObject jsonObjectLead = (JSONObject)jsonObjectLeads.get(j);
				  
				  for(int i = 0; i < listCampos.size(); i++) {
					  String coluna = listCampos.get(i);
					  coluna = coluna.trim();
					  
					  try {
						  Object value = (Object)jsonObjectLead.get(coluna);
					      colunasInsert = colunasInsert + virgula + coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      if(value != null)
					         listValores.add(value.toString());
					      else {
					    	  value = "null";
					    	  listValores.add(value);
					      }
					      
					   
					      
					      
					  }catch (Exception e) {}
					  
				  }
				 
				  
				  
				  
				 
				  String insert = "insert into "+tabela+" ("+colunasInsert+") values ("+valoresInsert+")!#!"+UUID.randomUUID();
					 
				  list.put(insert, listValores);
				  //insereContato(insert, listValores, id);
				  
				  
			  }
			  
			  
			  conn.disconnect();
			}
			 
			else if (conn.getResponseCode() == 429) {
					conn.disconnect();
					// esse status eh quando atigiu o limite de requisicao
					escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
					Thread.sleep(60000*30);
					//list = new ArrayList<String>();
					listConfiguration( method, listCampos,  tabela);
	     }else {
	    	 escreveLog("Status listConfiguration : "+conn.getResponseCode());
	     }
		  } catch (Exception e) {escreveLog("Erro ao buscar listConfiguration : "+e.toString());} 
		
		 return list;
		
	}
	
	
	/**
	 * Busca todos os leads, carregando os ids
	 * @param page
	 * @return
	 */
	public HashMap<String, SalesAccount> listAllAccounts(int page, HashMap<String, SalesAccount> list) {
	
		JSONParser parser = new JSONParser();
		try {
			
			String	u = "https://impulse.freshsales.io/api/sales_accounts/view/3000568842?include=industry_type,business_type,owner&page="+page;
			
         	URL url = new URL(u);
			
         	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Token token=wPBMcHQSK9yfCi3omDVGUQ");

			if (conn.getResponseCode() == 200) {
			
              BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
              String output;
              StringBuffer strBuffer = new StringBuffer();
			  while ((output = br.readLine()) != null) {
				  strBuffer.append(output);
			  }
			  
			  
			  Object obj =  parser.parse(strBuffer.toString());
			  JSONObject jsonObject = (JSONObject) obj;
			  
			   jsonObjectUsers = (JSONArray) jsonObject.get("users");
			   JSONArray jsonObjectBusinessType = (JSONArray) jsonObject.get("business_types");
			   JSONArray jsonObjectIndustryType = (JSONArray) jsonObject.get("industry_types");
			   jsonObjectTerritories = (JSONArray) jsonObject.get("territories");
			  
			  JSONArray jsonObjectLeads = (JSONArray) jsonObject.get("sales_accounts");
			  for(int j = 0 ; j < jsonObjectLeads.size(); j++) {
				  
				  SalesAccount salesAccount = new SalesAccount();  
				  JSONObject jsonObjectLead = (JSONObject)jsonObjectLeads.get(j);
				  
				  JSONObject jsonObjectCustom =  (JSONObject)jsonObjectLead.get("custom_field");
			  
				  for(int i = 0; i < listFieldsAccount.size(); i++) {
					  String coluna = listFieldsAccount.get(i);
					  coluna = coluna.trim();
					  try {
						  Object value = (Object)jsonObjectLead.get(coluna);
					      if(value != null) {
					    	 if(value.toString().trim().equals("")) value = "null"; 
					      }
					      else {
					    	  value = "null";
					      }
					      if(coluna.trim().equalsIgnoreCase("id")) salesAccount.setSales_account_id(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("last_contacted")) salesAccount.setSales_account_last_contacted(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("last_contacted_mode")) salesAccount.setSales_account_last_contacted_mode(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("last_contacted_sales_activity_mode")) salesAccount.setSales_account_last_contacted_sales_activity_mode(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("name")) salesAccount.setSales_account_name(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("number_of_employees")) salesAccount.setSales_account_number_of_employees(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("open_deals_amount")) salesAccount.setSales_account_open_deals_amount(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("updated_at")) salesAccount.setSales_account_updated_at(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("website")) salesAccount.setSales_account_website(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("city")) salesAccount.setSales_account_city(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("address")) salesAccount.setSales_account_address(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("country")) salesAccount.setSales_account_country(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("phone")) salesAccount.setSales_account_phone(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("state")) salesAccount.setSales_account_state(value.toString());
					      else if(coluna.trim().equalsIgnoreCase("zipcode")) salesAccount.setSales_account_zipcode(value.toString());
					      
					      
					      if(coluna.trim().equalsIgnoreCase("sales_account_cf_colaboradores")) {
					    	try { 
					    	  Object valuec = (Object)jsonObjectCustom.get("cf_colaboradores");
					    	  if(valuec != null) {
							    	 if(valuec.toString().trim().equals("")) valuec = "null"; 
							      }
							      else {
							    	  valuec = "null";
							      }
					    	  
					    	  if(value.toString().equalsIgnoreCase("true")) {
					    		  value = "1";
					    	  }else {
					    		  value = "0";
					    	  }
					    	  
					    	  salesAccount.setSales_account_cf_colaboradores(valuec.toString());
					    	}catch (Exception e) {
								// TODO: handle exception
							}
					      }
					      
					      
					      if(coluna.equals("owner_id")) {
					      
						      if(jsonObjectUsers.size() > 0) {
						    	  for(int k = 0 ; k < jsonObjectUsers.size(); k++) {
									  JSONObject jsonUser =  (JSONObject)jsonObjectUsers.get(k);
									  Object owner_display_name = (Object)jsonUser.get("display_name");
									  Object owner_email = (Object)jsonUser.get("email");
									  Object owner_mobile_number = (Object)jsonUser.get("mobile_number");
									  Object owner_is_active = (Object)jsonUser.get("is_active");
									  Object owner_work_number = (Object)jsonUser.get("work_number");
									  Object owner_id = (Object)jsonUser.get("id");
									  
									  if(owner_id.toString().equals(value.toString())) {
										  value = owner_display_name;
										  if(value != null) {
										    	 if(value.toString().trim().equals("")) value = "null"; 
										  }
										  else value = "null";
										  salesAccount.setSales_account_owner_display_name(value.toString());
										  value = owner_email;
										  if(value != null) {
										    	 if(value.toString().trim().equals("")) value = "null"; 
										  }
										  else value = "null";
										  salesAccount.setSales_account_owner_email(value.toString());
										  value = owner_mobile_number;
										  if(value != null) {
										    	 if(value.toString().trim().equals("")) value = "null"; 
										  }
										  else value = "null";
										  salesAccount.setSales_account_owner_mobile_number(value.toString());
										  value = owner_is_active;
										  if(value != null) {
										    	 if(value.toString().trim().equals("")) value = "null"; 
										  }
										  else value = "null";
										  salesAccount.setSales_account_owner_is_active(value.toString());
										  value = owner_work_number;
										  if(value != null) {
										    	 if(value.toString().trim().equals("")) value = "null"; 
										  }
										  else value = "null";
										  salesAccount.setSales_account_owner_work_number(value.toString());
										  value = owner_id;
										  if(value != null) {
										    	 if(value.toString().trim().equals("")) value = "null"; 
										  }
										  else value = "null";
										  salesAccount.setSales_account_owner_id(value.toString());
										     
									  }
							  
						         }
							  }
						      
					      
					      }
					      
					      if(coluna.equals("industry_type_id")) {
					      
						      if(jsonObjectIndustryType.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectIndustryType.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectIndustryType.get(k);
								  Object industry_type_name = (Object)jsonObj.get("name");
								  Object industry_type_id = (Object)jsonObj.get("id");
								  
								  if(industry_type_id.toString().equals(value.toString())) {
								  
									  value = industry_type_name;
									  if(value != null) {
									    	 if(value.toString().trim().equals("")) value = "null"; 
									  }
									  else value = "null";
									  salesAccount.setSales_account_industry_type_name(value.toString());
									  value = industry_type_id;
									  if(value != null) {
									    	 if(value.toString().trim().equals("")) value = "null"; 
									  }
									  else value = "null";
									  salesAccount.setSales_account_industry_type_id(value.toString());
				
									  
									
								  }
						        }
							  }
					    }
					   
					      if(coluna.equals("business_type_id")) {
						      
						      if(jsonObjectBusinessType.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectBusinessType.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectBusinessType.get(k);
								  Object business_type_name = (Object)jsonObj.get("name");
								  Object business_type_id = (Object)jsonObj.get("id");
								  
								  if(business_type_id.toString().equals(value.toString())) {
								  
									  value = business_type_name;
									  if(value != null) {
									    	 if(value.toString().trim().equals("")) value = "null"; 
									  }
									  else value = "null";
									  salesAccount.setSales_account_business_type_name(value.toString());
									  value = business_type_id;
									  if(value != null) {
									    	 if(value.toString().trim().equals("")) value = "null"; 
									  }
									  else value = "null";
									  salesAccount.setSales_account_business_type_id(value.toString());
				
									  
									
								  }
						        }
							  }
					    }
					    
					    if(coluna.equals("territory_id")) {
					    
					    
							    if(jsonObjectTerritories.size() > 0) {
							    	for(int k = 0 ; k < jsonObjectTerritories.size(); k++) {
									  JSONObject jsonObj =  (JSONObject)jsonObjectTerritories.get(k);
									  Object territory_name = (Object)jsonObj.get("name");
									   Object territory_id = (Object)jsonObj.get("id");
									  
									  if(territory_id.toString().equals(value.toString())) {
									  
										  value = territory_name;
										  if(value != null) {
										    	 if(value.toString().trim().equals("")) value = "null"; 
										  }
										  else value = "null";
										  salesAccount.setSales_account_territory_name(value.toString());
										  value = territory_id;
										  if(value != null) {
										    	 if(value.toString().trim().equals("")) value = "null"; 
										  }
										  else value = "null";
										  salesAccount.setSales_account_territory_id(value.toString());
										
									  }
							    	}
								   
								  }
					    }
					    
					   
					      
					  }catch (Exception e) {
						 // System.out.println("Erro aqui");
					     //e.printStackTrace();
					  }
					  
				  }
				  
				  
				 
				   list.put(salesAccount.getSales_account_id(), salesAccount);
			  
			  }
			  
			  
			  conn.disconnect();
			}else if (conn.getResponseCode() == 429) {
						conn.disconnect();
						// esse status eh quando atigiu o limite de requisicao
						escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
						Thread.sleep(60000*30);
						//list = new ArrayList<String>();
						listAllAccounts(page, list); 
		     }else {
		    	 conn.disconnect();
					// esse status eh quando atigiu o limite de requisicao
					escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
					Thread.sleep(60000*30);
					//list = new ArrayList<String>();
					listAllAccounts(page, list); 
		     }
			
		  } catch (Exception e) {escreveLog("Erro ao buscar listAllAccounts : "+e.toString());} 
		
		 return list;
		
	}
	
	
	
	public HashMap<String, List<Object>> listAllDeals1(int page, HashMap<String, SalesAccount> mapSalesAccount, HashMap<String, List<Object>> list) {
		
		JSONParser parser = new JSONParser();
		try {
			
			String	u = "https://impulse.freshsales.io/api/deals/view/3000841438?include=lead,owner,lead_stage,creater,updater,source,territory,campaign,deal_stage,deal_reason,deal_type,source,contacts,probability,sales_account&page="+page;
			
         	URL url = new URL(u);
			
         	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Token token=wPBMcHQSK9yfCi3omDVGUQ");

			if (conn.getResponseCode() == 200) {
			
              BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
              String output;
              StringBuffer strBuffer = new StringBuffer();
			  while ((output = br.readLine()) != null) {
				  strBuffer.append(output);
			  }
			  
			  String colunasInsert = "";
			  String valoresInsert = "";
			  String virgula = "";
			  List<Object> listValores = new ArrayList<Object>();
			  
			  Object obj =  parser.parse(strBuffer.toString());
			  JSONObject jsonObject = (JSONObject) obj;
			  
			   jsonObjectUsers = (JSONArray) jsonObject.get("users");
			  jsonObjectLeadStages = (JSONArray) jsonObject.get("deal_stages");
			  jsonObjectLeadReason = (JSONArray) jsonObject.get("deal_reasons");
			   jsonObjectLeadSource = (JSONArray) jsonObject.get("lead_source");
			   jsonObjectTerritories = (JSONArray) jsonObject.get("territories");
			  jsonObjectCampaigns = (JSONArray) jsonObject.get("campaigns");
			  JSONArray  jsonObjectdeal_types = (JSONArray) jsonObject.get("deal_types");
			  JSONArray  jsonObjectcontacts = (JSONArray) jsonObject.get("contacts");
			  JSONArray  jsonObjectsalesaccount = (JSONArray) jsonObject.get("sales_accounts");
			  JSONArray jsonObjectLeads = (JSONArray) jsonObject.get("deals");
			  for(int j = 0 ; j < jsonObjectLeads.size(); j++) {
				  
				  
				   String idDeal = ""; 
				   colunasInsert = "";
				   valoresInsert = "";
				   virgula = "";
				   listValores = new ArrayList<Object>();
				  
				  JSONObject jsonObjectLead = (JSONObject)jsonObjectLeads.get(j);
			      Deal deal = new Deal();
				  for(int i = 0; i < listFieldsDeal.size(); i++) {
					  String coluna = listFieldsDeal.get(i);
					  coluna = coluna.trim();
					  try {
						  Object value = (Object)jsonObjectLead.get(coluna);
					      colunasInsert = colunasInsert + virgula + coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      if(value != null) {
					    	 if(value.toString().trim().equals("")) value = "null"; 
					         
					    	 if(!value.equals("null")) {
						    	 if(coluna.equals("closed_date")) {
						    		 value = value + "T00:00:00-03:00";
						    	 }
	                             if(coluna.equals("expected_close")) {
	                            	 value = value + "T00:00:00-03:00";
						    	 }
					    	 }
					    	 
					    	 if(value.equals("null")) {
					    	  if(listFieldsDealDate.contains(coluna)) {
					    		  value = "NULL"; 
					    	  }
					    	 }
					    	 
					    	 listValores.add(value.toString());
					      }
					      else {
					    	  value = "null";
					    	  
					    	  if(listFieldsDealDate.contains(coluna)) {
					    		  value = "NULL"; 
					    	  }
					    	  
					    	  listValores.add(value);
					      }
					      if(coluna.equals("id")) {
					    	  idDeal = value.toString();
					      }
					      if(coluna.equals("name")) {
					    	  deal.setName(value.toString());
					      }
					      if(coluna.equals("amount")) {
					    	  deal.setAmount(value.toString());
					      }
					      
					      
					      if(coluna.equals("owner_id")) {
					      
						      if(jsonObjectUsers.size() > 0) {
						    	  for(int k = 0 ; k < jsonObjectUsers.size(); k++) {
									  JSONObject jsonUser =  (JSONObject)jsonObjectUsers.get(k);
									  Object owner_display_name = (Object)jsonUser.get("display_name");
									  Object owner_email = (Object)jsonUser.get("email");
									  Object owner_mobile_number = (Object)jsonUser.get("mobile_number");
									  Object owner_is_active = (Object)jsonUser.get("is_active");
									  Object owner_work_number = (Object)jsonUser.get("work_number");
									  Object owner_id = (Object)jsonUser.get("id");
									  
									  if(owner_id.toString().equals(value.toString())) {
											  if(owner_display_name != null) listValores.add(owner_display_name.toString()); else { owner_display_name = "null"; listValores.add(owner_display_name); }
											  if(owner_email != null) listValores.add(owner_email.toString()); else {  owner_email = "null"; listValores.add(owner_email); }
											  if(owner_mobile_number != null) listValores.add(owner_mobile_number.toString()); else { owner_mobile_number = "null"; listValores.add(owner_mobile_number); }
											  if(owner_is_active != null)listValores.add(owner_is_active.toString()); else { owner_is_active = "null"; listValores.add(owner_is_active); }
											  if(owner_work_number != null)listValores.add(owner_work_number.toString()); else { owner_work_number = "null"; listValores.add(owner_work_number); }
											  
											  colunasInsert = colunasInsert + virgula + "owner_display_name";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_email";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_mobile_number";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_is_active";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_work_number";
										      valoresInsert = valoresInsert + virgula + "?";
										     
									  }
							  
						         }
							  }
						      
					      
					      }
					      
					      
					      if(coluna.equals("territory_id")) {
							    
							    
							    if(jsonObjectTerritories.size() > 0) {
							    	for(int k = 0 ; k < jsonObjectTerritories.size(); k++) {
									  JSONObject jsonObj =  (JSONObject)jsonObjectTerritories.get(k);
									  Object territory_name = (Object)jsonObj.get("name");
									  Object territory_partial = (Object)jsonObj.get("partial");
									  Object territory_position = (Object)jsonObj.get("position");
									  Object territory_id = (Object)jsonObj.get("id");
									  
									  if(territory_id.toString().equals(value.toString())) {
									  
										  if(territory_name != null)  listValores.add(territory_name.toString());   else { territory_name = "null"; listValores.add(territory_name); }
										  if(territory_partial != null)   listValores.add(territory_partial.toString());  else { territory_partial = "null"; listValores.add(territory_partial); }
										  if(territory_position != null)  listValores.add(territory_position.toString());  else {  territory_position = "null"; listValores.add(territory_position); }
										 
										  colunasInsert = colunasInsert + virgula + "territory_name";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "territory_partial";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "territory_position";
									      valoresInsert = valoresInsert + virgula + "?";
								      
									  }
							    	}
								   
								  }
					    }
					    
					    if(coluna.equals("campaign_id")) {
					    
						    if(jsonObjectCampaigns.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectCampaigns.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectCampaigns.get(k);
								  Object campaign_name = (Object)jsonObj.get("name");
								  Object campaign_id = (Object)jsonObj.get("id");
								  if(campaign_id.toString().equals(value.toString())) {
								  
										  if(campaign_name != null)   listValores.add(campaign_name.toString());  else {  campaign_name = "null"; listValores.add(campaign_name); }
										  colunasInsert = colunasInsert + virgula + "campaign_name";
									      valoresInsert = valoresInsert + virgula + "?";
								  }
						    }
							  }
					    }
					    
					    
					    if(coluna.equals("sales_account_id")) {
						    
						    if(mapSalesAccount.size() > 0) {
						       Iterator<String> it = mapSalesAccount.keySet().iterator();
						       while(it.hasNext()) {
						    	   String key = it.next();
						    	   if(key.toString().equals(value.toString())) {
						    		   SalesAccount salesAccount =  mapSalesAccount.get(key);
						    		   
						    		   if(salesAccount.getSales_account_address() != null)   listValores.add(salesAccount.getSales_account_address().toString());  else { String campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_business_type_id() != null)   listValores.add(salesAccount.getSales_account_business_type_id().toString());  else {   String campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_business_type_name() != null)   listValores.add(salesAccount.getSales_account_business_type_name().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		  // if(salesAccount.getSales_account_cf_avaliados() != null)   listValores.add(salesAccount.getSales_account_cf_avaliados().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		  // if(salesAccount.getSales_account_cf_nome_da_empresa() != null)   listValores.add(salesAccount.getSales_account_cf_nome_da_empresa().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		  // if(salesAccount.getSales_account_cf_numero_de_funcionarios() != null)   listValores.add(salesAccount.getSales_account_cf_numero_de_funcionarios().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_city() != null)   listValores.add(salesAccount.getSales_account_city().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_country() != null)   listValores.add(salesAccount.getSales_account_country().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_industry_type_id() != null)   listValores.add(salesAccount.getSales_account_industry_type_id().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_industry_type_name() != null)   listValores.add(salesAccount.getSales_account_industry_type_name().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_last_contacted() != null)   listValores.add(salesAccount.getSales_account_last_contacted().toString());  else {  
						    			   String  campaign_name = "NULL"; 
						    			   //if(listFieldsDealDate.contains(coluna)) {
									    		 // value = "NULL"; 
									    	 // }
						    			   listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_last_contacted_mode() != null)   listValores.add(salesAccount.getSales_account_last_contacted_mode().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_last_contacted_sales_activity_mode() != null)   listValores.add(salesAccount.getSales_account_last_contacted_sales_activity_mode().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_name() != null)   listValores.add(salesAccount.getSales_account_name().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_number_of_employees() != null)   listValores.add(salesAccount.getSales_account_number_of_employees().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_open_deals_amount() != null)   listValores.add(salesAccount.getSales_account_open_deals_amount().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_display_name() != null)   listValores.add(salesAccount.getSales_account_owner_display_name().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_email() != null)   listValores.add(salesAccount.getSales_account_owner_email().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_id() != null)   listValores.add(salesAccount.getSales_account_owner_id().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_is_active() != null)   listValores.add(salesAccount.getSales_account_owner_is_active().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_mobile_number() != null)   listValores.add(salesAccount.getSales_account_owner_mobile_number().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_work_number() != null)   listValores.add(salesAccount.getSales_account_owner_work_number().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_phone() != null)   listValores.add(salesAccount.getSales_account_phone().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_state() != null)   listValores.add(salesAccount.getSales_account_state().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_territory_id() != null)   listValores.add(salesAccount.getSales_account_territory_id().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_territory_name() != null)   listValores.add(salesAccount.getSales_account_territory_name().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_updated_at() != null)   listValores.add(salesAccount.getSales_account_updated_at().toString());  else { 
						    			   String  campaign_name = "NULL"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_website() != null)   listValores.add(salesAccount.getSales_account_website().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_zipcode() != null)   listValores.add(salesAccount.getSales_account_zipcode().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   
						    		   
						    		   if(salesAccount.getSales_account_cf_colaboradores() != null)   listValores.add(salesAccount.getSales_account_cf_colaboradores().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						   			
						    		  colunasInsert = colunasInsert + virgula + "sales_account_address"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_business_type_id"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_business_type_name"; valoresInsert = valoresInsert + virgula + "?";
						    		//  colunasInsert = colunasInsert + virgula + "sales_account_cf_avaliados"; valoresInsert = valoresInsert + virgula + "?";
						    		//  colunasInsert = colunasInsert + virgula + "sales_account_cf_nome_da_empresa"; valoresInsert = valoresInsert + virgula + "?";
						    		//  colunasInsert = colunasInsert + virgula + "sales_account_cf_numero_de_funcionarios"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_city"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_country"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_industry_type_id"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_industry_type_name"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_last_contacted"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_last_contacted_mode"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_last_contacted_sales_activity_mode"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_name"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_number_of_employees"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_open_deals_amount"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_display_name"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_email"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_id"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_is_active"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_mobile_number"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_work_number"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_phone"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_state"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_territory_id"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_territory_name"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_updated_at"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_website"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_zipcode"; valoresInsert = valoresInsert + virgula + "?";
						    		  
						    		  colunasInsert = colunasInsert + virgula + "sales_account_cf_colaboradores"; valoresInsert = valoresInsert + virgula + "?";
						    	   }
						       }
						    	
							}
					    }
					    
					    if(coluna.equals("deal_type_id")) {
						    
						    if(jsonObjectdeal_types.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectdeal_types.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectdeal_types.get(k);
								  Object campaign_name = (Object)jsonObj.get("name");
								  Object campaign_id = (Object)jsonObj.get("id");
								  if(campaign_id.toString().equals(value.toString())) {
								  
										  if(campaign_name != null)   listValores.add(campaign_name.toString());  else {  campaign_name = "null"; listValores.add(campaign_name); }
										  colunasInsert = colunasInsert + virgula + "deal_type_name";
									      valoresInsert = valoresInsert + virgula + "?";
								  }
						    }
							  }
					    }
					    
					    if(coluna.equals("deal_reason_id")) {
						      
						      if(jsonObjectLeadReason.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectLeadReason.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectLeadReason.get(k);
								  Object lead_stages_name = (Object)jsonObj.get("name");
								  Object lead_stages_id = (Object)jsonObj.get("id");
								  
								  if(lead_stages_id.toString().equals(value.toString())) {
								  		 	 
									  if(lead_stages_name != null) listValores.add(lead_stages_name.toString()); else { lead_stages_name = "null"; listValores.add(lead_stages_name); }
											
							
								      colunasInsert = colunasInsert + virgula + "deal_reason_name";
								      valoresInsert = valoresInsert + virgula + "?";
								    
								  }
						        }
							      
							  }
					    }
					      
					      if(coluna.equals("deal_stage_id")) {
					      
						      if(jsonObjectLeadStages.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectLeadStages.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectLeadStages.get(k);
								  Object lead_stages_choice_type = (Object)jsonObj.get("choice_type");
								  Object deal_pipeline_id = (Object)jsonObj.get("deal_pipeline_id");
								  Object forecast_type = (Object)jsonObj.get("forecast_type");
								  Object lead_stages_name = (Object)jsonObj.get("name");
								 
								  Object lead_stages_partial = (Object)jsonObj.get("partial");
								  Object lead_stages_position = (Object)jsonObj.get("position");
								  Object lead_stages_id = (Object)jsonObj.get("id");
								  Object updated_at = (Object)jsonObj.get("updated_at");
								  
								  if(lead_stages_id.toString().equals(value.toString())) {
								  
									  if(lead_stages_choice_type != null) listValores.add(lead_stages_choice_type.toString()); else { lead_stages_choice_type = "null"; listValores.add(lead_stages_choice_type); }
									  if(deal_pipeline_id != null) listValores.add(deal_pipeline_id.toString()); else { deal_pipeline_id = "null"; listValores.add(deal_pipeline_id); }
									  if(forecast_type != null) listValores.add(forecast_type.toString()); else { forecast_type = "null"; listValores.add(forecast_type); }
										 	 
									  if(lead_stages_name != null) listValores.add(lead_stages_name.toString()); else { lead_stages_name = "null"; listValores.add(lead_stages_name); }
									  if(lead_stages_partial != null) listValores.add(lead_stages_partial.toString()); else { lead_stages_partial = "null"; listValores.add(lead_stages_partial); }
									  if(lead_stages_position != null)  listValores.add(lead_stages_position.toString()); else { lead_stages_position = "null"; listValores.add(lead_stages_position); }
									  if(updated_at != null)  listValores.add(updated_at.toString()); else { updated_at = "NULL"; listValores.add(updated_at); }
										
								  
									  colunasInsert = colunasInsert + virgula + "deal_stage_choice_type";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_deal_pipeline_id";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_forecast_type";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_name";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_partial";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_position";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_updated_at";
								      valoresInsert = valoresInsert + virgula + "?";
								  }
						        }
							      
							  }
					    }
					   
					    if(coluna.equals("lead_source_id")) {  
					      
						      if(jsonObjectLeadSource.size() > 0) {
						    	  for(int k = 0 ; k < jsonObjectLeadSource.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectLeadSource.get(k);
								  Object lead_source_name = (Object)jsonObj.get("name");
								  Object lead_source_partial = (Object)jsonObj.get("partial");
								  Object lead_source_position = (Object)jsonObj.get("position");
								  Object lead_source_id = (Object)jsonObj.get("id");
								  
								  if(lead_source_id.toString().equals(value.toString())) {
								  
									  if(lead_source_name != null)   listValores.add(lead_source_name.toString()); else { lead_source_name = "null"; listValores.add(lead_source_name); }
									  if(lead_source_partial != null)   listValores.add(lead_source_partial.toString()); else { lead_source_partial = "null"; listValores.add(lead_source_partial); }
									  if(lead_source_position != null)  listValores.add(lead_source_position.toString()); else { lead_source_position = "null"; listValores.add(lead_source_position); }
									
									  colunasInsert = colunasInsert + virgula + "lead_source_name";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "lead_source_partial";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "lead_source_position";
								      valoresInsert = valoresInsert + virgula + "?";
						         }
						      }
							  }
					      }
					    
					    
					      
					  }catch (Exception e) {
						///  System.out.println("Erro aqui");
					   //  e.printStackTrace();
					  }
					  
				  }
				  
				  JSONArray jsonContatcs =  (JSONArray)jsonObjectLead.get("contact_ids");
				  for(int k = 0; k < jsonContatcs.size(); k++) {
					  Object id_contact = jsonContatcs.get(k);
					  if(mapContatoDeal.containsKey(idDeal)) {
						  List<String> listc = mapContatoDeal.get(idDeal);
						  listc.add(id_contact.toString());
						  mapContatoDeal.remove(idDeal);
						  mapContatoDeal.put(idDeal, listc);
					  }else {
						  List<String> listc = new ArrayList<String>();
						  listc.add(id_contact.toString());
						  mapContatoDeal.put(idDeal, listc);
					  }
				  }
				  
				  
				  JSONObject jsonObjectCustom =  (JSONObject)jsonObjectLead.get("custom_field");
				  for(int i = 0; i < listFieldsCustomDeal.size(); i++) {
					  String coluna = listFieldsCustomDeal.get(i);
					  try {
						  Object value = (Object)jsonObjectCustom.get(coluna);
						 
					      colunasInsert = colunasInsert + virgula + coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      if(value != null) {
					    	  if(value.toString().equalsIgnoreCase("true")) {
					    		  value = "1";
					    	  }else {
					    		  value = "0";
					    	  }
					         listValores.add(value.toString());
					      } else {
					    	  value = "0";
					    	  listValores.add(value);
					      }
					  }catch (Exception e) {}
					  
				  }
				  
				  
				  
						 
				 String insert = "insert into freshsales_deal ("+colunasInsert+") values ("+valoresInsert+")!#!"+UUID.randomUUID();
				 
				 List<Object> listAux = new ArrayList<Object>();
				 for(int i = 0 ; i < listValores.size(); i++) {
					 String str = listValores.get(i).toString();
					 if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-03:00") != -1) {
						 str = str.replaceAll("T", " ");
						 str = str.replaceAll("-03:00", "");
						 str = str.trim();
						 listAux.add(str); 
					 }else if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-02:00") != -1) {
						 str = str.replaceAll("T", " ");
						 str = str.replaceAll("-02:00", "");
						 str = str.trim();
						 listAux.add(str); 
					 }else {
						 listAux.add(str); 
					 }
				 }
				 listValores = new ArrayList<Object>();
				 for(int i = 0 ; i < listAux.size(); i++) {
					 listValores.add(listAux.get(i)); 
				 }
				 
				 list.put(insert, listValores);  
				// insereLead(insert, listValores, id);
				 
				 
				 mapDealContato.put(idDeal, deal);
			  
			  }
			  
			  
			  conn.disconnect();
			}else if (conn.getResponseCode() == 429) {
						conn.disconnect();
						// esse status eh quando atigiu o limite de requisicao
						escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
						Thread.sleep(60000*30);
						//list = new ArrayList<String>();
						listAllDeals1(page, mapSalesAccount, list); 
		     }else {
		    	 conn.disconnect();
					// esse status eh quando atigiu o limite de requisicao
					escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
					Thread.sleep(60000*30);
					//list = new ArrayList<String>();
					listAllDeals1(page, mapSalesAccount, list); 
		     }
			
		  } catch (Exception e) {escreveLog("Erro ao buscar listAllDeals : "+e.toString());} 
		
		 return list;
		
	}
	
	
	
	public HashMap<String, List<Object>> listAllDeals2(int page, HashMap<String, SalesAccount> mapSalesAccount, HashMap<String, List<Object>> list) {
		
		JSONParser parser = new JSONParser();
		try {
			
			String	u = "https://impulse.freshsales.io/api/deals/view/3001084847?include=lead,owner,lead_stage,creater,updater,source,territory,campaign,deal_stage,deal_reason,deal_type,source,contacts,probability,sales_account&page="+page;
			
         	URL url = new URL(u);
			
         	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Token token=wPBMcHQSK9yfCi3omDVGUQ");

			if (conn.getResponseCode() == 200) {
			
              BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
              String output;
              StringBuffer strBuffer = new StringBuffer();
			  while ((output = br.readLine()) != null) {
				  strBuffer.append(output);
			  }
			  
			  String colunasInsert = "";
			  String valoresInsert = "";
			  String virgula = "";
			  List<Object> listValores = new ArrayList<Object>();
			  
			  Object obj =  parser.parse(strBuffer.toString());
			  JSONObject jsonObject = (JSONObject) obj;
			  
			   jsonObjectUsers = (JSONArray) jsonObject.get("users");
			  jsonObjectLeadStages = (JSONArray) jsonObject.get("deal_stages");
			  jsonObjectLeadReason = (JSONArray) jsonObject.get("deal_reasons");
			   jsonObjectLeadSource = (JSONArray) jsonObject.get("lead_source");
			   jsonObjectTerritories = (JSONArray) jsonObject.get("territories");
			  jsonObjectCampaigns = (JSONArray) jsonObject.get("campaigns");
			  JSONArray  jsonObjectdeal_types = (JSONArray) jsonObject.get("deal_types");
			  JSONArray  jsonObjectcontacts = (JSONArray) jsonObject.get("contacts");
			  JSONArray  jsonObjectsalesaccount = (JSONArray) jsonObject.get("sales_accounts");
			  JSONArray jsonObjectLeads = (JSONArray) jsonObject.get("deals");
			  for(int j = 0 ; j < jsonObjectLeads.size(); j++) {
				  
				  
				   String idDeal = ""; 
				   colunasInsert = "";
				   valoresInsert = "";
				   virgula = "";
				   listValores = new ArrayList<Object>();
				  
				  JSONObject jsonObjectLead = (JSONObject)jsonObjectLeads.get(j);
			      Deal deal = new Deal();
				  for(int i = 0; i < listFieldsDeal.size(); i++) {
					  String coluna = listFieldsDeal.get(i);
					  coluna = coluna.trim();
					  try {
						  Object value = (Object)jsonObjectLead.get(coluna);
					      colunasInsert = colunasInsert + virgula + coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      if(value != null) {
					    	 if(value.toString().trim().equals("")) value = "null"; 
					         
					    	 if(!value.equals("null")) {
						    	 if(coluna.equals("closed_date")) {
						    		 value = value + "T00:00:00-03:00";
						    	 }
	                             if(coluna.equals("expected_close")) {
	                            	 value = value + "T00:00:00-03:00";
						    	 }
					    	 }
					    	 
					    	 if(value.equals("null")) {
					    	 if(listFieldsDealDate.contains(coluna)) {
					    		  value = "NULL"; 
					    	  }
					    	 }
					    	 
					    	 listValores.add(value.toString());
					      }
					      else {
					    	  value = "null";
					    	  
					    	  if(listFieldsDealDate.contains(coluna)) {
					    		  value = "NULL"; 
					    	  }
					    	  
					    	  listValores.add(value);
					      }
					      if(coluna.equals("id")) {
					    	  idDeal = value.toString();
					      }
					      if(coluna.equals("name")) {
					    	  deal.setName(value.toString());
					      }
					      if(coluna.equals("amount")) {
					    	  deal.setAmount(value.toString());
					      }
					      
					      
					      if(coluna.equals("owner_id")) {
					      
						      if(jsonObjectUsers.size() > 0) {
						    	  for(int k = 0 ; k < jsonObjectUsers.size(); k++) {
									  JSONObject jsonUser =  (JSONObject)jsonObjectUsers.get(k);
									  Object owner_display_name = (Object)jsonUser.get("display_name");
									  Object owner_email = (Object)jsonUser.get("email");
									  Object owner_mobile_number = (Object)jsonUser.get("mobile_number");
									  Object owner_is_active = (Object)jsonUser.get("is_active");
									  Object owner_work_number = (Object)jsonUser.get("work_number");
									  Object owner_id = (Object)jsonUser.get("id");
									  
									  if(owner_id.toString().equals(value.toString())) {
											  if(owner_display_name != null) listValores.add(owner_display_name.toString()); else { owner_display_name = "null"; listValores.add(owner_display_name); }
											  if(owner_email != null) listValores.add(owner_email.toString()); else {  owner_email = "null"; listValores.add(owner_email); }
											  if(owner_mobile_number != null) listValores.add(owner_mobile_number.toString()); else { owner_mobile_number = "null"; listValores.add(owner_mobile_number); }
											  if(owner_is_active != null)listValores.add(owner_is_active.toString()); else { owner_is_active = "null"; listValores.add(owner_is_active); }
											  if(owner_work_number != null)listValores.add(owner_work_number.toString()); else { owner_work_number = "null"; listValores.add(owner_work_number); }
											  
											  colunasInsert = colunasInsert + virgula + "owner_display_name";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_email";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_mobile_number";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_is_active";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_work_number";
										      valoresInsert = valoresInsert + virgula + "?";
										     
									  }
							  
						         }
							  }
						      
					      
					      }
					      
					      
					      if(coluna.equals("territory_id")) {
							    
							    
							    if(jsonObjectTerritories.size() > 0) {
							    	for(int k = 0 ; k < jsonObjectTerritories.size(); k++) {
									  JSONObject jsonObj =  (JSONObject)jsonObjectTerritories.get(k);
									  Object territory_name = (Object)jsonObj.get("name");
									  Object territory_partial = (Object)jsonObj.get("partial");
									  Object territory_position = (Object)jsonObj.get("position");
									  Object territory_id = (Object)jsonObj.get("id");
									  
									  if(territory_id.toString().equals(value.toString())) {
									  
										  if(territory_name != null)  listValores.add(territory_name.toString());   else { territory_name = "null"; listValores.add(territory_name); }
										  if(territory_partial != null)   listValores.add(territory_partial.toString());  else { territory_partial = "null"; listValores.add(territory_partial); }
										  if(territory_position != null)  listValores.add(territory_position.toString());  else {  territory_position = "null"; listValores.add(territory_position); }
										 
										  colunasInsert = colunasInsert + virgula + "territory_name";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "territory_partial";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "territory_position";
									      valoresInsert = valoresInsert + virgula + "?";
								      
									  }
							    	}
								   
								  }
					    }
					    
					    if(coluna.equals("campaign_id")) {
					    
						    if(jsonObjectCampaigns.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectCampaigns.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectCampaigns.get(k);
								  Object campaign_name = (Object)jsonObj.get("name");
								  Object campaign_id = (Object)jsonObj.get("id");
								  if(campaign_id.toString().equals(value.toString())) {
								  
										  if(campaign_name != null)   listValores.add(campaign_name.toString());  else {  campaign_name = "null"; listValores.add(campaign_name); }
										  colunasInsert = colunasInsert + virgula + "campaign_name";
									      valoresInsert = valoresInsert + virgula + "?";
								  }
						    }
							  }
					    }
					    
					    
					    if(coluna.equals("sales_account_id")) {
						    
						    if(mapSalesAccount.size() > 0) {
						       Iterator<String> it = mapSalesAccount.keySet().iterator();
						       while(it.hasNext()) {
						    	   String key = it.next();
						    	   if(key.toString().equals(value.toString())) {
						    		   SalesAccount salesAccount =  mapSalesAccount.get(key);
						    		   
						    		   if(salesAccount.getSales_account_address() != null)   listValores.add(salesAccount.getSales_account_address().toString());  else { String campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_business_type_id() != null)   listValores.add(salesAccount.getSales_account_business_type_id().toString());  else {   String campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_business_type_name() != null)   listValores.add(salesAccount.getSales_account_business_type_name().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		  // if(salesAccount.getSales_account_cf_avaliados() != null)   listValores.add(salesAccount.getSales_account_cf_avaliados().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		  // if(salesAccount.getSales_account_cf_nome_da_empresa() != null)   listValores.add(salesAccount.getSales_account_cf_nome_da_empresa().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		  // if(salesAccount.getSales_account_cf_numero_de_funcionarios() != null)   listValores.add(salesAccount.getSales_account_cf_numero_de_funcionarios().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_city() != null)   listValores.add(salesAccount.getSales_account_city().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_country() != null)   listValores.add(salesAccount.getSales_account_country().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_industry_type_id() != null)   listValores.add(salesAccount.getSales_account_industry_type_id().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_industry_type_name() != null)   listValores.add(salesAccount.getSales_account_industry_type_name().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_last_contacted() != null)   listValores.add(salesAccount.getSales_account_last_contacted().toString());  else {  String  campaign_name = "NULL"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_last_contacted_mode() != null)   listValores.add(salesAccount.getSales_account_last_contacted_mode().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_last_contacted_sales_activity_mode() != null)   listValores.add(salesAccount.getSales_account_last_contacted_sales_activity_mode().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_name() != null)   listValores.add(salesAccount.getSales_account_name().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_number_of_employees() != null)   listValores.add(salesAccount.getSales_account_number_of_employees().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_open_deals_amount() != null)   listValores.add(salesAccount.getSales_account_open_deals_amount().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_display_name() != null)   listValores.add(salesAccount.getSales_account_owner_display_name().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_email() != null)   listValores.add(salesAccount.getSales_account_owner_email().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_id() != null)   listValores.add(salesAccount.getSales_account_owner_id().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_is_active() != null)   listValores.add(salesAccount.getSales_account_owner_is_active().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_mobile_number() != null)   listValores.add(salesAccount.getSales_account_owner_mobile_number().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_owner_work_number() != null)   listValores.add(salesAccount.getSales_account_owner_work_number().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_phone() != null)   listValores.add(salesAccount.getSales_account_phone().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_state() != null)   listValores.add(salesAccount.getSales_account_state().toString());  else { String   campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_territory_id() != null)   listValores.add(salesAccount.getSales_account_territory_id().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_territory_name() != null)   listValores.add(salesAccount.getSales_account_territory_name().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_updated_at() != null)   listValores.add(salesAccount.getSales_account_updated_at().toString());  else {  String  campaign_name = "NULL"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_website() != null)   listValores.add(salesAccount.getSales_account_website().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   if(salesAccount.getSales_account_zipcode() != null)   listValores.add(salesAccount.getSales_account_zipcode().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						    		   
						    		   
						    		   if(salesAccount.getSales_account_cf_colaboradores() != null)   listValores.add(salesAccount.getSales_account_cf_colaboradores().toString());  else {  String  campaign_name = "null"; listValores.add(campaign_name); }
						   			
						    		  colunasInsert = colunasInsert + virgula + "sales_account_address"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_business_type_id"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_business_type_name"; valoresInsert = valoresInsert + virgula + "?";
						    		//  colunasInsert = colunasInsert + virgula + "sales_account_cf_avaliados"; valoresInsert = valoresInsert + virgula + "?";
						    		//  colunasInsert = colunasInsert + virgula + "sales_account_cf_nome_da_empresa"; valoresInsert = valoresInsert + virgula + "?";
						    		//  colunasInsert = colunasInsert + virgula + "sales_account_cf_numero_de_funcionarios"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_city"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_country"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_industry_type_id"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_industry_type_name"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_last_contacted"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_last_contacted_mode"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_last_contacted_sales_activity_mode"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_name"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_number_of_employees"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_open_deals_amount"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_display_name"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_email"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_id"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_is_active"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_mobile_number"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_owner_work_number"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_phone"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_state"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_territory_id"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_territory_name"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_updated_at"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_website"; valoresInsert = valoresInsert + virgula + "?";
						    		  colunasInsert = colunasInsert + virgula + "sales_account_zipcode"; valoresInsert = valoresInsert + virgula + "?";
						    		  
						    		  colunasInsert = colunasInsert + virgula + "sales_account_cf_colaboradores"; valoresInsert = valoresInsert + virgula + "?";
						    	   }
						       }
						    	
							}
					    }
					    
					    if(coluna.equals("deal_type_id")) {
						    
						    if(jsonObjectdeal_types.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectdeal_types.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectdeal_types.get(k);
								  Object campaign_name = (Object)jsonObj.get("name");
								  Object campaign_id = (Object)jsonObj.get("id");
								  if(campaign_id.toString().equals(value.toString())) {
								  
										  if(campaign_name != null)   listValores.add(campaign_name.toString());  else {  campaign_name = "null"; listValores.add(campaign_name); }
										  colunasInsert = colunasInsert + virgula + "deal_type_name";
									      valoresInsert = valoresInsert + virgula + "?";
								  }
						    }
							  }
					    }
					    
					    if(coluna.equals("deal_reason_id")) {
						      
						      if(jsonObjectLeadReason.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectLeadReason.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectLeadReason.get(k);
								  Object lead_stages_name = (Object)jsonObj.get("name");
								  Object lead_stages_id = (Object)jsonObj.get("id");
								  
								  if(lead_stages_id.toString().equals(value.toString())) {
								  		 	 
									  if(lead_stages_name != null) listValores.add(lead_stages_name.toString()); else { lead_stages_name = "null"; listValores.add(lead_stages_name); }
											
							
								      colunasInsert = colunasInsert + virgula + "deal_reason_name";
								      valoresInsert = valoresInsert + virgula + "?";
								    
								  }
						        }
							      
							  }
					    }
					      
					      if(coluna.equals("deal_stage_id")) {
					      
						      if(jsonObjectLeadStages.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectLeadStages.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectLeadStages.get(k);
								  Object lead_stages_choice_type = (Object)jsonObj.get("choice_type");
								  Object deal_pipeline_id = (Object)jsonObj.get("deal_pipeline_id");
								  Object forecast_type = (Object)jsonObj.get("forecast_type");
								  Object lead_stages_name = (Object)jsonObj.get("name");
								 
								  Object lead_stages_partial = (Object)jsonObj.get("partial");
								  Object lead_stages_position = (Object)jsonObj.get("position");
								  Object lead_stages_id = (Object)jsonObj.get("id");
								  Object updated_at = (Object)jsonObj.get("updated_at");
								  
								  if(lead_stages_id.toString().equals(value.toString())) {
								  
									  if(lead_stages_choice_type != null) listValores.add(lead_stages_choice_type.toString()); else { lead_stages_choice_type = "null"; listValores.add(lead_stages_choice_type); }
									  if(deal_pipeline_id != null) listValores.add(deal_pipeline_id.toString()); else { deal_pipeline_id = "null"; listValores.add(deal_pipeline_id); }
									  if(forecast_type != null) listValores.add(forecast_type.toString()); else { forecast_type = "null"; listValores.add(forecast_type); }
										 	 
									  if(lead_stages_name != null) listValores.add(lead_stages_name.toString()); else { lead_stages_name = "null"; listValores.add(lead_stages_name); }
									  if(lead_stages_partial != null) listValores.add(lead_stages_partial.toString()); else { lead_stages_partial = "null"; listValores.add(lead_stages_partial); }
									  if(lead_stages_position != null)  listValores.add(lead_stages_position.toString()); else { lead_stages_position = "null"; listValores.add(lead_stages_position); }
									  if(updated_at != null)  listValores.add(updated_at.toString()); else { updated_at = "NULL"; listValores.add(updated_at); }
										
								  
									  colunasInsert = colunasInsert + virgula + "deal_stage_choice_type";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_deal_pipeline_id";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_forecast_type";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_name";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_partial";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_position";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "deal_stage_updated_at";
								      valoresInsert = valoresInsert + virgula + "?";
								  }
						        }
							      
							  }
					    }
					   
					    if(coluna.equals("lead_source_id")) {  
					      
						      if(jsonObjectLeadSource.size() > 0) {
						    	  for(int k = 0 ; k < jsonObjectLeadSource.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectLeadSource.get(k);
								  Object lead_source_name = (Object)jsonObj.get("name");
								  Object lead_source_partial = (Object)jsonObj.get("partial");
								  Object lead_source_position = (Object)jsonObj.get("position");
								  Object lead_source_id = (Object)jsonObj.get("id");
								  
								  if(lead_source_id.toString().equals(value.toString())) {
								  
									  if(lead_source_name != null)   listValores.add(lead_source_name.toString()); else { lead_source_name = "null"; listValores.add(lead_source_name); }
									  if(lead_source_partial != null)   listValores.add(lead_source_partial.toString()); else { lead_source_partial = "null"; listValores.add(lead_source_partial); }
									  if(lead_source_position != null)  listValores.add(lead_source_position.toString()); else { lead_source_position = "null"; listValores.add(lead_source_position); }
									
									  colunasInsert = colunasInsert + virgula + "lead_source_name";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "lead_source_partial";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "lead_source_position";
								      valoresInsert = valoresInsert + virgula + "?";
						         }
						      }
							  }
					      }
					    
					    
					      
					  }catch (Exception e) {
						///  System.out.println("Erro aqui");
					   //  e.printStackTrace();
					  }
					  
				  }
				  
				  JSONArray jsonContatcs =  (JSONArray)jsonObjectLead.get("contact_ids");
				  for(int k = 0; k < jsonContatcs.size(); k++) {
					  Object id_contact = jsonContatcs.get(k);
					  if(mapContatoDeal.containsKey(idDeal)) {
						  List<String> listc = mapContatoDeal.get(idDeal);
						  listc.add(id_contact.toString());
						  mapContatoDeal.remove(idDeal);
						  mapContatoDeal.put(idDeal, listc);
					  }else {
						  List<String> listc = new ArrayList<String>();
						  listc.add(id_contact.toString());
						  mapContatoDeal.put(idDeal, listc);
					  }
				  }
				  
				  
				  JSONObject jsonObjectCustom =  (JSONObject)jsonObjectLead.get("custom_field");
				  for(int i = 0; i < listFieldsCustomDeal.size(); i++) {
					  String coluna = listFieldsCustomDeal.get(i);
					  try {
						  Object value = (Object)jsonObjectCustom.get(coluna);
						 
					      colunasInsert = colunasInsert + virgula + coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      if(value != null) {
					    	  if(value.toString().equalsIgnoreCase("true")) {
					    		  value = "1";
					    	  }else {
					    		  value = "0";
					    	  }
					         listValores.add(value.toString());
					      } else {
					    	  value = "0";
					    	  listValores.add(value);
					      }
					  }catch (Exception e) {}
					  
				  }
				  
				  
				  
						 
				 String insert = "insert into freshsales_deal ("+colunasInsert+") values ("+valoresInsert+")!#!"+UUID.randomUUID();
				 
				 List<Object> listAux = new ArrayList<Object>();
				 for(int i = 0 ; i < listValores.size(); i++) {
					 String str = listValores.get(i).toString();
					 if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-03:00") != -1) {
						 str = str.replaceAll("T", " ");
						 str = str.replaceAll("-03:00", "");
						 str = str.trim();
						 listAux.add(str); 
					 }else if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-02:00") != -1) {
						 str = str.replaceAll("T", " ");
						 str = str.replaceAll("-02:00", "");
						 str = str.trim();
						 listAux.add(str); 
					 }else {
						 listAux.add(str); 
					 }
				 }
				 listValores = new ArrayList<Object>();
				 for(int i = 0 ; i < listAux.size(); i++) {
					 listValores.add(listAux.get(i)); 
				 }
				 
				 list.put(insert, listValores);  
				// insereLead(insert, listValores, id);
				 
				 
				 mapDealContato.put(idDeal, deal);
			  
			  }
			  
			  
			  conn.disconnect();
			}else if (conn.getResponseCode() == 429) {
						conn.disconnect();
						// esse status eh quando atigiu o limite de requisicao
						escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
						Thread.sleep(60000*30);
						//list = new ArrayList<String>();
						listAllDeals2(page, mapSalesAccount, list); 
		     }else {
		    	 conn.disconnect();
					// esse status eh quando atigiu o limite de requisicao
					escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
					Thread.sleep(60000*30);
					//list = new ArrayList<String>();
					listAllDeals2(page, mapSalesAccount, list); 
		     }
			
		  } catch (Exception e) {escreveLog("Erro ao buscar listAllDeals : "+e.toString());} 
		
		 return list;
		
	}
	
	
	
	public HashMap<String, List<Object>> listAllLeads(int page,  List<String> listIds, HashMap<String, List<Object>> list) {
		
		JSONParser parser = new JSONParser();
		try {
			//3000568811 - Carrega os ultimos 
			
			//3000568810 - Carrega todos os leads
			
		String	u = "https://impulse.freshsales.io/api/leads/view/3000568811?include=lead,owner,lead_stage,lead_reason,creater,updater,source,territory,campaign&page="+page;
			
			
         	URL url = new URL(u);
			
         	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Token token=wPBMcHQSK9yfCi3omDVGUQ");

			if (conn.getResponseCode() == 200) {
			
              BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
              String output;
              StringBuffer strBuffer = new StringBuffer();
			  while ((output = br.readLine()) != null) {
				  strBuffer.append(output);
			  }
			  
			  String colunasInsert = "";
			  String valoresInsert = "";
			  String virgula = "";
			  List<Object> listValores = new ArrayList<Object>();
			  
			  Object obj =  parser.parse(strBuffer.toString());
			  JSONObject jsonObject = (JSONObject) obj;
			  
			  //if(page == 1) {
			   jsonObjectUsers = (JSONArray) jsonObject.get("users");
			   jsonObjectLeadStages = (JSONArray) jsonObject.get("lead_stages");
			   jsonObjectLeadReason = (JSONArray) jsonObject.get("lead_reasons");
			   jsonObjectLeadSource = (JSONArray) jsonObject.get("lead_source");
			   jsonObjectTerritories = (JSONArray) jsonObject.get("territories");
			   jsonObjectCampaigns = (JSONArray) jsonObject.get("campaigns");
			 // }
			  
			  JSONArray jsonObjectLeads = (JSONArray) jsonObject.get("leads");
			  for(int j = 0 ; j < jsonObjectLeads.size(); j++) {
				  
				  
				  
				   colunasInsert = "";
				   valoresInsert = "";
				   virgula = "";
				   listValores = new ArrayList<Object>();
				   String id = "";
				  
				  JSONObject jsonObjectLead = (JSONObject)jsonObjectLeads.get(j);
			  
				  for(int i = 0; i < listFieldsLead.size(); i++) {
					  String coluna = listFieldsLead.get(i);
					  coluna = coluna.trim();
					  
					  if(coluna.equals("tags")) {
					      
					     try {
					    	 JSONArray jsonTags = (JSONArray)jsonObjectLead.get(coluna); 
					    	 String tags = "";
					    	 String virg = "";
					    	  for(int p = 0 ; p < jsonTags.size(); p++) {
					    		  try {
					    		  JSONObject objTag = (JSONObject)jsonTags.get(p);
					    		  Object value = (Object)objTag.get("value");
					    		  if(value != null) {
					    			  tags = tags + virg + value;
					    			  virg = ";";
					    		  }
					    		  }catch (Exception e) {
									// TODO: handle exception
								}
					    	  }
					    	  if(!tags.equals("")) {
					    		  
					    		  colunasInsert = colunasInsert + virgula + coluna;
							      valoresInsert = valoresInsert + virgula + "?";
							      virgula = ",";
					    		  
					    		  listValores.add(tags);
					    	  }
					    	 
					     }catch (Exception e) {
							// TODO: handle exception
						}
						     
					  }else {
						  
					  try {
						  Object value = (Object)jsonObjectLead.get(coluna);
					      colunasInsert = colunasInsert + virgula + coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      
					      if(coluna.equalsIgnoreCase("cf_data_de_retorno_standby")) {
					    	  try {
					    		  if(value.toString().trim().equals("0")) {
					    			  value = "null";
					    		  }
					    	  }catch (Exception e) {
								// TODO: handle exception
							}
					      }
					      
					      
					      
					      if(value != null) {
					    	 if(value.toString().trim().equals("")) value = "null"; 
					        
					    	 
					    	 
					    	 if(value.equals("null")) {
					    	   if(listFieldsLeadDate.contains(coluna)) {
					    		  value = "NULL";
					    	   }
					    	 }
					    	
					    	 listValores.add(value.toString());
					      }
					      else {
					    	  value = "null";
					    	  
					    	  if(listFieldsLeadDate.contains(coluna)) {
					    		  value = "NULL";
					    	  }
					    	  
					    	  
					    	  
					    	  listValores.add(value);
					      }
					      
					       if(coluna.equalsIgnoreCase("id")) {
					    	   if(!listIds.contains(value.toString())) {
					    		   listIds.add(value.toString());
					    	   }
					         
					         id = value.toString();
					      }
					      
					      if(coluna.equals("owner_id")) {
					      
						      if(jsonObjectUsers.size() > 0) {
						    	  for(int k = 0 ; k < jsonObjectUsers.size(); k++) {
									  JSONObject jsonUser =  (JSONObject)jsonObjectUsers.get(k);
									  Object owner_display_name = (Object)jsonUser.get("display_name");
									  Object owner_email = (Object)jsonUser.get("email");
									  Object owner_mobile_number = (Object)jsonUser.get("mobile_number");
									  Object owner_is_active = (Object)jsonUser.get("is_active");
									  Object owner_work_number = (Object)jsonUser.get("work_number");
									  Object owner_id = (Object)jsonUser.get("id");
									  
									  if(owner_id.toString().equals(value.toString())) {
											  if(owner_display_name != null) listValores.add(owner_display_name.toString()); else { owner_display_name = "null"; listValores.add(owner_display_name); }
											  if(owner_email != null) listValores.add(owner_email.toString()); else {  owner_email = "null"; listValores.add(owner_email); }
											  if(owner_mobile_number != null) listValores.add(owner_mobile_number.toString()); else { owner_mobile_number = "null"; listValores.add(owner_mobile_number); }
											  if(owner_is_active != null)listValores.add(owner_is_active.toString()); else { owner_is_active = "null"; listValores.add(owner_is_active); }
											  if(owner_work_number != null)listValores.add(owner_work_number.toString()); else { owner_work_number = "null"; listValores.add(owner_work_number); }
											  
											  colunasInsert = colunasInsert + virgula + "owner_display_name";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_email";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_mobile_number";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_is_active";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "owner_work_number";
										      valoresInsert = valoresInsert + virgula + "?";
										     
									  }
							  
						         }
							  }
						      
					      
					      }
					      
					      if(coluna.equals("lead_reason_id")) {
						      
						      if(jsonObjectLeadReason.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectLeadReason.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectLeadReason.get(k);
								  Object lead_reason_name = (Object)jsonObj.get("name");
								  Object lead_reason_id = (Object)jsonObj.get("id");
								  
								  if(lead_reason_id.toString().equals(value.toString())) {
								  
									  if(lead_reason_name != null) listValores.add(lead_reason_name.toString()); else { lead_reason_name = "null"; listValores.add(lead_reason_name); }
									
								  
									    colunasInsert = colunasInsert + virgula + "lead_reason_name";
								      valoresInsert = valoresInsert + virgula + "?";
								    
								  }
						        }
							      
							  }
					    }
					      
					      if(coluna.equals("lead_stage_id")) {
					      
						      if(jsonObjectLeadStages.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectLeadStages.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectLeadStages.get(k);
								  Object lead_stages_choice_type = (Object)jsonObj.get("choice_type");
								  Object lead_stages_name = (Object)jsonObj.get("name");
								 
								  Object lead_stages_partial = (Object)jsonObj.get("partial");
								  Object lead_stages_position = (Object)jsonObj.get("position");
								  Object lead_stages_id = (Object)jsonObj.get("id");
								  
								  if(lead_stages_id.toString().equals(value.toString())) {
								  
									  if(lead_stages_choice_type != null) listValores.add(lead_stages_choice_type.toString()); else { lead_stages_choice_type = "null"; listValores.add(lead_stages_choice_type); }
									  if(lead_stages_name != null) listValores.add(lead_stages_name.toString()); else { lead_stages_name = "null"; listValores.add(lead_stages_name); }
									  if(lead_stages_partial != null) listValores.add(lead_stages_partial.toString()); else { lead_stages_partial = "null"; listValores.add(lead_stages_partial); }
									  if(lead_stages_position != null)  listValores.add(lead_stages_position.toString()); else { lead_stages_position = "null"; listValores.add(lead_stages_position); }
									
								  
									  colunasInsert = colunasInsert + virgula + "lead_stage_choice_type";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "lead_stage_name";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "lead_stage_partial";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "lead_stage_position";
								      valoresInsert = valoresInsert + virgula + "?";
								  }
						        }
							      
							  }
					    }
					   
					    if(coluna.equals("lead_source_id")) {  
					      
						      if(jsonObjectLeadSource.size() > 0) {
						    	  for(int k = 0 ; k < jsonObjectLeadSource.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectLeadSource.get(k);
								  Object lead_source_name = (Object)jsonObj.get("name");
								  Object lead_source_partial = (Object)jsonObj.get("partial");
								  Object lead_source_position = (Object)jsonObj.get("position");
								  Object lead_source_id = (Object)jsonObj.get("id");
								  
								  if(lead_source_id.toString().equals(value.toString())) {
								  
									  if(lead_source_name != null)   listValores.add(lead_source_name.toString()); else { lead_source_name = "null"; listValores.add(lead_source_name); }
									  if(lead_source_partial != null)   listValores.add(lead_source_partial.toString()); else { lead_source_partial = "null"; listValores.add(lead_source_partial); }
									  if(lead_source_position != null)  listValores.add(lead_source_position.toString()); else { lead_source_position = "null"; listValores.add(lead_source_position); }
									
									  colunasInsert = colunasInsert + virgula + "lead_source_name";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "lead_source_partial";
								      valoresInsert = valoresInsert + virgula + "?";
								      colunasInsert = colunasInsert + virgula + "lead_source_position";
								      valoresInsert = valoresInsert + virgula + "?";
						         }
						      }
							  }
					      }
					    
					    if(coluna.equals("territory_id")) {
					    
					    
							    if(jsonObjectTerritories.size() > 0) {
							    	for(int k = 0 ; k < jsonObjectTerritories.size(); k++) {
									  JSONObject jsonObj =  (JSONObject)jsonObjectTerritories.get(k);
									  Object territory_name = (Object)jsonObj.get("name");
									  Object territory_partial = (Object)jsonObj.get("partial");
									  Object territory_position = (Object)jsonObj.get("position");
									  Object territory_id = (Object)jsonObj.get("id");
									  
									  if(territory_id.toString().equals(value.toString())) {
									  
										  if(territory_name != null)  listValores.add(territory_name.toString());   else { territory_name = "null"; listValores.add(territory_name); }
										  if(territory_partial != null)   listValores.add(territory_partial.toString());  else { territory_partial = "null"; listValores.add(territory_partial); }
										  if(territory_position != null)  listValores.add(territory_position.toString());  else {  territory_position = "null"; listValores.add(territory_position); }
										 
										  colunasInsert = colunasInsert + virgula + "territory_name";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "territory_partial";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "territory_position";
									      valoresInsert = valoresInsert + virgula + "?";
								      
									  }
							    	}
								   
								  }
					    }
					    
					    if(coluna.equals("campaign_id")) {
					    
						    if(jsonObjectCampaigns.size() > 0) {
						    	for(int k = 0 ; k < jsonObjectCampaigns.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectCampaigns.get(k);
								  Object campaign_name = (Object)jsonObj.get("name");
								  Object campaign_id = (Object)jsonObj.get("id");
								  if(campaign_id.toString().equals(value.toString())) {
								  
										  if(campaign_name != null)   listValores.add(campaign_name.toString());  else {  campaign_name = "null"; listValores.add(campaign_name); }
										  colunasInsert = colunasInsert + virgula + "campaign_name";
									      valoresInsert = valoresInsert + virgula + "?";
								  }
						    }
							  }
					    }
					      
					  }catch (Exception e) {
						 // System.out.println("Erro aqui");
					     //e.printStackTrace();
					  }
				  } 
				  }
				  
				  JSONObject jsonObjectCompany =  (JSONObject)jsonObjectLead.get("company");
				  for(int i = 0; i < listFieldsCompany.size(); i++) {
					  String coluna = listFieldsCompany.get(i);
					  try {
						  
						  if(coluna.equals("industry_type")) {
							  JSONObject jsonObjectCompanyInd =  (JSONObject)jsonObjectCompany.get(coluna);
							  Object value = (Object)jsonObjectCompanyInd.get("name");
						      colunasInsert = colunasInsert + virgula + "company_"+coluna;
						      valoresInsert = valoresInsert + virgula + "?";
						      virgula = ",";
						      if(value != null)
						         listValores.add(value.toString());
						      else {
						    	  value = "null";
						    	  listValores.add(value);
						      }
						  }else {
							  Object value = (Object)jsonObjectCompany.get(coluna);
						      colunasInsert = colunasInsert + virgula + "company_"+coluna;
						      valoresInsert = valoresInsert + virgula + "?";
						      virgula = ",";
						      if(value != null)
						         listValores.add(value.toString());
						      else {
						    	  value = "null";
						    	  listValores.add(value);
						      }
						  }
					  }catch (Exception e) {
						  
						  colunasInsert = colunasInsert + virgula + "company_"+coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      listValores.add("null");
						  
						  
					  }
					  
				  }
				  JSONObject jsonObjectCustom =  (JSONObject)jsonObjectLead.get("custom_field");
				  for(int i = 0; i < listFieldsCustom.size(); i++) {
					  String coluna = listFieldsCustom.get(i);
					  try {
						  Object value = (Object)jsonObjectCustom.get(coluna);
						  if(coluna.equals("cf_e-mail_alternativo")) {
							  coluna = "cf_email_alternativo";
						  }
						  if(coluna.equals("cf_cadastro_manual_-_erro_integracao")) {
							  coluna = "cf_cadastro_manual_erro_integracao";
						  }
						  
						  if(coluna.equals("cf_acessou_nine-box")) {
							  coluna = "cf_acessou_nine_box";
						  }
						  
						  if(coluna.equals("cf_cadastro_manual_-_erro_integracao")) {
							  coluna = "cf_cadastro_manual_erro_integracao";
						  }
						  
						  if(coluna.equals("cf_desafio (company)")) {
							  coluna = "cf_desafio";
						  }
						  if(coluna.equals("cf_subsetor_de_ti (company)")) {
							  coluna = "cf_subsetor_de_ti";
						  }
						  
						  
					      colunasInsert = colunasInsert + virgula + coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      if(value != null) {
					    	  if(value.toString().equalsIgnoreCase("true")) {
					    		  value = "1";
					    	  }else if(value.toString().equalsIgnoreCase("false")) {
					    		  value = "0";
					    	  }
					    	  
					    	  if(coluna.equals("cf_data_de_retorno_standby")) {
					    		  if(value.toString().trim().equals("")) {
					    			  value = "0";
					    		  }
					    		  
					    		  if(value.equals("0")) {
					    			  value = "NULL";
					    		  }
					    	  }
					    	  
					    	  
					    	  
					         listValores.add(value.toString());
					      } else {
					    	  value = "0";
					    	  
					    	  if(coluna.equals("cf_data_de_retorno_standby")) {
					    		  if(value.toString().trim().equals("")) {
					    			  value = "0";
					    		  }
					    		  
					    		  if(value.equals("0")) {
					    			  value = "NULL";
					    		  }
					    	  }
					    	  
					    	  
					    	  listValores.add(value);
					      }
					  }catch (Exception e) { System.out.println("nao achou a coluna : "+coluna);}
					  
				  }
				  
						 
				 String insert = "insert into freshsales_lead ("+colunasInsert+", type_name) values ("+valoresInsert+", 'lead')!#!"+UUID.randomUUID();
				 List<Object> listAux = new ArrayList<Object>();
				 for(int i = 0 ; i < listValores.size(); i++) {
					 String str = listValores.get(i).toString();
					 if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-03:00") != -1) {
						 str = str.replaceAll("T", " ");
						 str = str.replaceAll("-03:00", "");
						 str = str.trim();
						 listAux.add(str); 
					 }else if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-02:00") != -1) {
						 str = str.replaceAll("T", " ");
						 str = str.replaceAll("-02:00", "");
						 str = str.trim();
						 listAux.add(str); 
					 }else {
						 listAux.add(str); 
					 }
				 }
				 listValores = new ArrayList<Object>();
				 for(int i = 0 ; i < listAux.size(); i++) {
					 listValores.add(listAux.get(i)); 
				 }
				 
				/* String[] aDAdos = insert.split("!#!");
				 String dados = aDAdos[0];
				 Iterator<String> it = list.keySet().iterator();
				 boolean bExiste = false;
				 while (it.hasNext()) {
					 String[] aKeys = it.next().split("!#!");
					 String key = aKeys[0];
					 if(key.equals(dados)) {
						 bExiste = true;
					 }
				 }
				 
				 if(!bExiste) */
				     list.put(insert, listValores);  
				 
				 // monta o update
				 String[] aDados = colunasInsert.split(",");
				 List<Object> listUpdate = new ArrayList<Object>();
				 String dadosCol = ""; String virg = "";
				 for(int i = 0; i < aDados.length; i++) {
					 if(!aDados[i].equals("id")) {
						 listUpdate.add(listValores.get(i)); 	 
					   dadosCol = dadosCol + virg + aDados[i] + " = ? ";
					   virg = ", ";
					 }
				 }
				 
				 String update = "update freshsales_lead set "+dadosCol+"  where id = '"+id+"'!#!"+UUID.randomUUID();
				 list.put(update, listUpdate);  
				 
				 
				// insereLead(insert, listValores, id);
			  
			  }
			  
			  
			  conn.disconnect();
			}else if (conn.getResponseCode() == 429) {
						conn.disconnect();
						// esse status eh quando atigiu o limite de requisicao
						escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
						Thread.sleep(60000*30);
						//list = new ArrayList<String>();
						listAllLeads(page, listIds, list); 
		     }else {
		    	 conn.disconnect();
					// esse status eh quando atigiu o limite de requisicao
					escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
					Thread.sleep(60000*30);
					//list = new ArrayList<String>();
					listAllLeads(page, listIds, list); 
		     }
			
		  } catch (Exception e) {escreveLog("Erro ao buscar listAllLeads : "+e.toString());} 
		
		 return list;
		
	}
	
	
		
	/**
	 * Ira carregar todos os contatos	
	 * @param page
	 * @return
	 */
	public HashMap<String, List<Object>> listAllContact(int page, HashMap<String, List<Object>> list) {
		
		JSONParser parser = new JSONParser();
		try {
			String u = "https://impulse.freshsales.io/api/contacts/view/3000568821?include=contact,owner,lead_stage,creater,updater,source,territory,campaign&page="+page;
         	
			//String u = "https://impulse.freshsales.io/api/contacts/3014437788";
			
			URL url = new URL(u);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Token token=wPBMcHQSK9yfCi3omDVGUQ");

			if (conn.getResponseCode() == 200) {
			
              BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
              String output;
              StringBuffer strBuffer = new StringBuffer();
			  while ((output = br.readLine()) != null) {
				  strBuffer.append(output);
			  }
			  Object obj =  parser.parse(strBuffer.toString());
			  JSONObject jsonObject = (JSONObject) obj;
			  String colunasInsert = "";
			  String  valoresInsert = "";
			  String virgula = "";
			  List<Object>  listValores = new ArrayList<Object>();
			//  if(page == 1) {
			   jsonObjectUsers = (JSONArray) jsonObject.get("users");
			   jsonObjectLeadSource = (JSONArray) jsonObject.get("lead_source");
			   jsonObjectTerritories = (JSONArray) jsonObject.get("territories");
			   jsonObjectCampaigns = (JSONArray) jsonObject.get("campaigns");
			  //}
			  
			  JSONArray jsonObjectLeads = (JSONArray) jsonObject.get("contacts");
			  for(int j = 0 ; j < jsonObjectLeads.size(); j++) {
				  
				   colunasInsert = "";
				   valoresInsert = "";
				   virgula = "";
				   listValores = new ArrayList<Object>();
				  
				  JSONObject jsonObjectLead = (JSONObject)jsonObjectLeads.get(j);
				  
				  for(int i = 0; i < listFieldsContact.size(); i++) {
					  String coluna = listFieldsContact.get(i);
					  coluna = coluna.trim();
					  
					  try {
						  Object value = (Object)jsonObjectLead.get(coluna);
						  
						  if(coluna.equals("cf_cadastro_manual")) {
							
							  colunasInsert = colunasInsert + virgula + "cf_cadastro_manual_erro_integracao";
						  }else {
							  colunasInsert = colunasInsert + virgula + coluna;
						  }
						  
					      
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      if(value != null) {
					    	  if(value.toString().trim().equals("")) value = "null";
					    	  
					    	  if(value.equals("null")) {
					    	    if(listFieldsLeadDate.contains(coluna)) {
					    		  value = "NULL";
					    	     }
					    	  }
					    	  
					         listValores.add(value.toString());
					      } else {
					    	  value = "null";
					    	  
					    	  
					    	  if(listFieldsLeadDate.contains(coluna)) {
					    		  value = "NULL";
					    	  }
					    	  
					    	  
					    	  listValores.add(value);
					      }
					      
					   
					      
					      
					      if(coluna.equals("owner_id")) {
					      
							      if(jsonObjectUsers.size() > 0) {
							    	  for(int k = 0 ; k < jsonObjectUsers.size(); k++) {
									  JSONObject jsonUser =  (JSONObject)jsonObjectUsers.get(k);
									  Object owner_display_name = (Object)jsonUser.get("display_name");
									  Object owner_email = (Object)jsonUser.get("email");
									  Object owner_mobile_number = (Object)jsonUser.get("mobile_number");
									  Object owner_is_active = (Object)jsonUser.get("is_active");
									  Object owner_work_number = (Object)jsonUser.get("work_number");
									  Object owner_id = (Object)jsonUser.get("id");
									  
									  if(owner_id.toString().trim().equals(value.toString().trim())) {
									  
										  if(owner_display_name != null) listValores.add(owner_display_name.toString()); else { listValores.add(owner_display_name); }
										  if(owner_email != null) listValores.add(owner_email.toString()); else { listValores.add(owner_email); }
										  if(owner_mobile_number != null) listValores.add(owner_mobile_number.toString()); else { listValores.add(owner_mobile_number); }
										  if(owner_is_active != null)listValores.add(owner_is_active.toString()); else { listValores.add(owner_is_active); }
										  if(owner_work_number != null)listValores.add(owner_work_number.toString()); else { listValores.add(owner_work_number); }
										  
										  colunasInsert = colunasInsert + virgula + "owner_display_name";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "owner_email";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "owner_mobile_number";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "owner_is_active";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "owner_work_number";
									      valoresInsert = valoresInsert + virgula + "?";
								      
									  }
							      }
								     
								  }
					      }
					      
					      if(coluna.equals("lead_source_id")) {
						   
								  if(jsonObjectLeadSource.size() > 0) {
									  for(int k = 0 ; k < jsonObjectLeadSource.size(); k++) {
									  JSONObject jsonObj =  (JSONObject)jsonObjectLeadSource.get(k);
									  Object lead_source_name = (Object)jsonObj.get("name");
									  Object lead_source_partial = (Object)jsonObj.get("partial");
									  Object lead_source_position = (Object)jsonObj.get("position");
									  Object lead_source_id = (Object)jsonObj.get("id");
									  
									  if(lead_source_id.toString().trim().equals(value.toString().trim())) {
									  
											  if(lead_source_name != null)   listValores.add(lead_source_name.toString()); else { listValores.add(lead_source_name); }
											  if(lead_source_partial != null)   listValores.add(lead_source_partial.toString()); else { listValores.add(lead_source_partial); }
											  if(lead_source_position != null)  listValores.add(lead_source_position.toString()); else { listValores.add(lead_source_position); }
											
											  colunasInsert = colunasInsert + virgula + "lead_source_name";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "lead_source_partial";
										      valoresInsert = valoresInsert + virgula + "?";
										      colunasInsert = colunasInsert + virgula + "lead_source_position";
										      valoresInsert = valoresInsert + virgula + "?";
								      
									  }
								  } 
								  }
					      }
					      
					      if(coluna.equals("territory_id")) {
						  
							  if(jsonObjectTerritories.size() > 0) {
								  for(int k = 0 ; k < jsonObjectTerritories.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectTerritories.get(k);
								  Object territory_name = (Object)jsonObj.get("name");
								  Object territory_partial = (Object)jsonObj.get("partial");
								  Object territory_position = (Object)jsonObj.get("position");
								  Object territory_id = (Object)jsonObj.get("id");
								  
								  if(territory_id.toString().trim().equals(value.toString().trim())) {
								  
										  if(territory_name != null)  listValores.add(territory_name.toString());   else { listValores.add(territory_name); }
										  if(territory_partial != null)   listValores.add(territory_partial.toString());  else { listValores.add(territory_partial); }
										  if(territory_position != null)  listValores.add(territory_position.toString());  else { listValores.add(territory_position); }
										
										  colunasInsert = colunasInsert + virgula + "territory_name";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "territory_partial";
									      valoresInsert = valoresInsert + virgula + "?";
									      colunasInsert = colunasInsert + virgula + "territory_position";
									      valoresInsert = valoresInsert + virgula + "?";
								  }
							  }
							  }
					      }
					      
					      if(coluna.equals("campaign_id")) {
						  
							  if(jsonObjectCampaigns.size() > 0) {
								  for(int k = 0 ; k < jsonObjectCampaigns.size(); k++) {
								  JSONObject jsonObj =  (JSONObject)jsonObjectCampaigns.get(k);
								  Object campaign_name = (Object)jsonObj.get("name");
								  Object campaign_id = (Object)jsonObj.get("id");
								  
								  if(campaign_id.toString().trim().equals(value.toString().trim())) {
								  
										  if(campaign_name != null)   listValores.add(campaign_name.toString());  else { listValores.add(campaign_name); }
										
										  colunasInsert = colunasInsert + virgula + "campaign_name";
									      valoresInsert = valoresInsert + virgula + "?";
							      
								  }
								  }
							  }
					      }
					      
					      
					  }catch (Exception e) {}
					  
				  }
				  JSONObject jsonObjectCompany =  (JSONObject)jsonObjectLead.get("company");
				  for(int i = 0; i < listFieldsCompany.size(); i++) {
					  String coluna = listFieldsCompany.get(i);
					  try {
						  Object value = (Object)jsonObjectCompany.get(coluna);
					       colunasInsert = colunasInsert + virgula + "company_"+coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      virgula = ",";
					      if(value != null)
					         listValores.add(value.toString());
					      else {
					    	  value = "null";
					    	  listValores.add(value);
					      }
					  }catch (Exception e) {}
					  
				  }
				  JSONObject jsonObjectCustom =  (JSONObject)jsonObjectLead.get("custom_field");
				  for(int i = 0; i < listFieldsCustomContact.size(); i++) {
					  String coluna = listFieldsCustomContact.get(i);
					  try {
						  Object value = (Object)jsonObjectCustom.get(coluna);
						  if(coluna.equals("cf_e-mail")) {
							  coluna = "cf_email_alternative";
						  }
					      colunasInsert = colunasInsert + virgula + coluna;
					      valoresInsert = valoresInsert + virgula + "?";
					      if(value != null) {
					    	  if(value.toString().equalsIgnoreCase("true")) {
					    		  value = "1";
					    	  }else {
					    		  value = "0";
					    	  }
					         listValores.add(value.toString());
					      }  else {
					    	  value = "0";
					    	  listValores.add(value);
					      }
					  }catch (Exception e) {}
					  
				  }
				  
				 
				  
				  
				  
				 
				  String insert = "insert into freshsales_lead ("+colunasInsert+", type_name, lead_stage_name) values ("+valoresInsert+", 'contact', 'Oportunidade')!#!"+UUID.randomUUID();
					
				  List<Object> listAux = new ArrayList<Object>();
					 for(int i = 0 ; i < listValores.size(); i++) {
						 Object s = listValores.get(i);
						 if(s == null) {
							 s = "null";
						 }
						 String str = s.toString();
						 if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-03:00") != -1) {
							 str = str.replaceAll("T", " ");
							 str = str.replaceAll("-03:00", "");
							 str = str.trim();
							 listAux.add(str); 
						 }else if(str.indexOf("-") != -1 && str.indexOf("T") != -1 && str.indexOf("-02:00") != -1) {
							 str = str.replaceAll("T", " ");
							 str = str.replaceAll("-02:00", "");
							 str = str.trim();
							 listAux.add(str); 
						 }else {
							 listAux.add(str); 
						 }
					 }
					 listValores = new ArrayList<Object>();
					 for(int i = 0 ; i < listAux.size(); i++) {
						 listValores.add(listAux.get(i)); 
					 }
				  
				  list.put(insert, listValores);
				  //insereContato(insert, listValores, id);
				  
				  
			  }
			  
			  
			  conn.disconnect();
			}
			 
			else if (conn.getResponseCode() == 429) {
					conn.disconnect();
					// esse status eh quando atigiu o limite de requisicao
					escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
					Thread.sleep(60000*30);
					//list = new ArrayList<String>();
					listAllContact(page, list); 
	     }else {
	    	 conn.disconnect();
				// esse status eh quando atigiu o limite de requisicao
				escreveLog("Atingiu o limite de requisicao, aguardando 30 min...");
				Thread.sleep(60000*30);
				//list = new ArrayList<String>();
				listAllContact(page, list); 
	     }
		  } catch (Exception e) {escreveLog("Erro ao buscar listAllContact : "+e.toString());
		  
		    System.out.println("Erro contato : "+e.toString());
		    e.printStackTrace();
		  
		  } 
		
		 return list;
		
	}
	
	public void carregaConexaoTeste() {
		try {
			String url = "jdbc:mysql://localhost:3306/impulse_freshsales";
			String user = "root";
			String pass = "123456";  //master
			Class.forName("com.mysql.jdbc.Driver");
			conexao = DriverManager.getConnection(url, user, pass);
			
			
	    } catch (Exception e) {
	    	escreveLog("Erro ao fazer a conexao : "+e.toString());
		}
	}
	
	public void carregaConexao() {
		try {
			String url = "jdbc:mysql://localhost:3306/impulse_freshsales";
			String user = "root";
			String pass = "master";  //master
			Class.forName("com.mysql.jdbc.Driver");
			conexao = DriverManager.getConnection(url, user, pass);
			
			
	    } catch (Exception e) {
	    	escreveLog("Erro ao fazer a conexao : "+e.toString());
		}
	}
	
	/**
	 * Escreve no arquivo de LOG
	 * @param log
	 */
	 public void escreveLog(String log){
		 //comentando para teste tirar depois
		 
		 if(arquivoLog == null){
			 
			 String data = Funcoes.retornaDataAtual();
			 String hora = Funcoes.retornaHoraAtual();
			 
			 try {
					arquivoLog = new FileWriter(new File(local+"IntegradorFreshsales"+data+"_"+hora+".log"));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		 }
		 
	 		Date data = new Date(); 
	 		try {
	 			arquivoLog.write(data+" - "+log+"\n");
	 			arquivoLog.flush();
			} catch (IOException e) {}
			
		
	   
	 
	 }
	 	 
	 	 

}
