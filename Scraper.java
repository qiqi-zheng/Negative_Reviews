//Save jsoup-1.11.3.jar file in /home/username then execute the following commands in PuTTY
//[username@msba-hadoop-name ~]$ export CLASSPATH="/home/username/jsoup-1.11.3.jar:."
//[username@msba-hadoop-name ~]$ javac RottenTomatoes.java
//[username@msba-hadoop-name ~]$ java RottenTomatoes

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.*;
import java.util.*;
import java.lang.Integer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Scraper {
	public static String getStarRatings(Element tr) {
		String stars = "0";

		Elements spans = tr.select("td").get(1).select("span");
		for (Element span : spans) {
			if (span.attr("class").equals("star fill")) {
				stars = span.text();
			}
		}
		return stars;
	}

	public static void main(String args[]){

		Document document, document2;
		
		try {
			//Get Document object after parsing the html from given url.
			System.out.println("running...");
			PrintWriter writer = new PrintWriter("airline_reviews.txt", "UTF-8");
			
			writer.println("date|overall rating|seat type|recommended|seat_comfort|cabin_staff_service|food_bev|ground_service|inflight_ent|wifi|money");
			for(int page=1;page<=2;page++) {
				document = Jsoup.connect("https://www.airlinequality.com/airline-reviews/united-airlines/page/" + String.valueOf(page) + "/").get();
				Elements reviews = document.select("article[itemprop=review]"); 
				
				for (Element review : reviews) {
					Elements table = review.getElementsByClass("review-ratings");
					Elements trs = table.get(0).select("tr");
					Elements span;
					String date = "";
					String overall_rating = review.select("span[itemprop=ratingValue]").text();
					String seat_type = "";
					String recommended = "";
					String seat_comfort = "0";
					String cabin_staff_service = "0";
					String food_bev = "0";
					String ground_service = "0";
					String inflight_ent = "0";
					String wifi = "0";
					String money = "0";

					for (Element tr : trs) {
						if (tr.select("td").get(0).text().equals("Date Flown"))
							date = tr.select("td").get(1).text();
						else if (tr.select("td").get(0).text().equals("Seat Type"))
							seat_type = tr.select("td").get(1).text();
						else if (tr.select("td").get(0).text().equals("Recommended"))
							recommended = tr.select("td").get(1).text();
						else if (tr.select("td").get(0).text().equals("Seat Comfort"))
							seat_comfort = getStarRatings(tr);
						else if (tr.select("td").get(0).text().equals("Cabin Staff Service"))
							cabin_staff_service = getStarRatings(tr);
						else if (tr.select("td").get(0).text().equals("Food & Beverages"))
							food_bev = getStarRatings(tr);
						else if (tr.select("td").get(0).text().equals("Ground Service"))
							ground_service = getStarRatings(tr);
						else if (tr.select("td").get(0).text().equals("Inflight Entertainment"))
							inflight_ent = getStarRatings(tr);
						else if (tr.select("td").get(0).text().equals("Wifi & Connectivity"))
							wifi = getStarRatings(tr);
						else if (tr.select("td").get(0).text().equals("Value For Money"))
							money = getStarRatings(tr);
					}
					writer.println(date + "|" + overall_rating + "|" + seat_type + "|" + recommended + "|" + seat_comfort + "|" +
						           cabin_staff_service + "|" + food_bev + "|" + inflight_ent + "|" + ground_service + "|" +
						           wifi + "|" + money);
				}	
			}
			writer.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("done!");
	}

}