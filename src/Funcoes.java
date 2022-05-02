import java.util.Date;

public class Funcoes {
	
	public static String retornaDataAtual() {
		String sData = "";
		String sMes = "";
		Date dtAtual = new Date();
        if (dtAtual != null) {
			int ano = dtAtual.getYear() + 1900;
			int mes = dtAtual.getMonth() + 1;
			if (Integer.toString(mes).length() == 1)
				sMes = "0" + Integer.toString(mes);
			else
				sMes = Integer.toString(mes);

			int dia = dtAtual.getDate();
			String sDia = Integer.toString(dia);
			if (sDia.length() == 1)
				sDia = "0" + sDia;

			sData = Integer.toString(ano)+"_"+sMes+"_"+sDia;
		}

		return sData;
	}
	
	
   public static String retornaHoraAtual() {
		
		Date time = new Date();
		int h = time.getHours();
		String hora = "";
		if (h < 10) {
			hora = "0" + h;
		} else {
			hora = Integer.toString(h);
		}

		int m = time.getMinutes();
		String minuto = "";
		if (m < 10) {
			minuto = "0" + m;
		} else {
			minuto = Integer.toString(m);
		}
		
		int s = time.getSeconds();
		String segundo = "";
		if (s < 10) {
			segundo = "0" + s;
		} else {
			segundo = Integer.toString(s);
		}
		
		//String horaAtual = Long.toString(time.getTime());
		
		String horaAtual = hora+"_"+minuto+"_"+segundo;

		return horaAtual;
	}
	
	
}
	
	
