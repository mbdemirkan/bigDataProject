# bigDataProject

Proje, Cryptingup'un api ile sunduğu verilerin NoSQL (MongoDB) üzerine 
kaydedilmesi ve kullanıcıya özet bilgi sunulması amacıyla yapılmıştır.

İlk çalışmada, yok ise, api üzerinden exchanges bilgilerini MongoDB'ye kaydeder.

Uygulama anlık verileri (assets ve markets) api üzerinden sürekli (her çekim arasında parametrik bir süre bekleyerek) 
Kafka'ya mesaj olarak iletilmesi ve Kafka'dan alınan verilerin MongoDB'ye kayıt edilmesi
şekilde tasarlanmıştır.

Projede veriler çoğunlukla Spark Dataframe'ler oluşturularak ve Spark SQL'ler kullanılarak işlenmiştir.

Sadece sahip olunan varlıklar için veya api ile sunulan tüm varlıklar için çalıştırılabilir.

Önyüz için Flask/SocketIO kullanılmıştır.

## Proje tasarımı

![alt text](design.png)

### Dosyalar ve içerikleri

* app.py: Önyüz için SocketIO/Flask uygulaması
* design.drawio: Yukarıdaki tasarımın çizim dosyası. https://app.diagrams.net/ adresinden açılabilir.
* design.png: Yukarıdaki tasarım çizimi 
* docker-compose.yml: Docker üzerinde birden fazla container çalıştırabilmek için kullanılan docker-compose dosyası. Kafka ve MongoDB için gerekli container tanımlarını içerir.
* example.png: Örnek bir çalıştırmanın ekran görüntüsü
* from_cryptingup_to_kafka.py: Cryptingup Api'lerinden alınan bilgilerin kafka'ya gönderilmesi ve Kafka'da tanımlı değil ise topicleri oluşturan Python dosyası 
* from_kafka_to_mongodb_asset.py: Kafka'dan anlık değişen asset bilgilerini MongoDB'ye kayıt eden Python dosyası
* from_kafka_to_mongodb_market.py: Kafka'dan anlık değişen market bilgilerini MongoDB'ye kayıt eden Python dosyası
* graphs.py: Grafikler için kodlarımı yazdığım Python dosyası
* index.html: Önyüz kodu
* my_library.py: Spark, Kafka ve MongoDB ile entegrasyonlar için kodlarımı yazdığım Python dosyası

## Projeyi çalıştırma adımları

### 1. Kafka'yı ve MongoDB'yi başlatabilmek için aşağıdaki komutu girin:  
>docker-compose up

Not: Makinanızda docker ve docker-compose yüklü olmalıdır.

### 2. Cryptingup Api'lerinden Kafka'ya mesajların gönderilmesi için aşağıdaki Python dosyasını çalıştırın:
> python cryptingup_to_kafka.py

### 3. Kafka'dan varlık değerlerini MongoDB'ye almak için aşağıdaki Python dosyasını çalıştırın:
> python from_kafka_to_mongodb_asset.py

### 4. Kafka'dan varlıkların marketlerdeki değerlerini MongoDB'ye almak için aşağıdaki Python dosyasını çalıştırın:
> python from_kafka_to_mongodb_market.py

### 5. SocketIO/Flask uygulaması için Python dosyasını çalıştırın:
> python app.py

### 6. Önyüze ulaşmak için tarayıcıdan aşağıdaki adrese giriş yapın:
> http://127.0.0.1:5000
