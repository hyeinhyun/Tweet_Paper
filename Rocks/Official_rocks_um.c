#include <rocksdb/c.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#define DATABASE "Rock_bag_man(TEST).db"
#include <assert.h>
typedef struct _kvs_tweet_t
{
  char id[2048];
  char content[4096];

} kvs_tweet_t;



double Insert_Data(const rocksdb_options_t* options, char* key,char* value)
{
  rocksdb_t *db;
  rocksdb_writeoptions_t *woptions;
  char *err = NULL;
  kvs_tweet_t emp;
  struct timeval startTime, endTime, gepTime;
  memset(&emp.id,'\0',sizeof(emp.id));
  memset(&emp.content,'\0',sizeof(emp.content));
  double operating_time;

 strcpy(emp.id,key);
 strcpy(emp.content,value);

  gettimeofday( &startTime, NULL );

  /* DB OPEN */
 db = rocksdb_open(options, DATABASE, &err);
 assert(!err);



  woptions = rocksdb_writeoptions_create();

  rocksdb_put(db, woptions,&emp.id, strlen(emp.id)+1, &emp.content, strlen(emp.content)+1, &err);


  gettimeofday( &endTime, NULL );
  assert(!err);


    //printf("  Record: '%s' -> '%s'\n",emp.id, emp.content);

    operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
    -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

    //printf("%f\n",operating_time);

//printf("ellapsed time %f second\n", operating_time);
rocksdb_close(db);
return operating_time;


}

double Insert_BatchData(const rocksdb_options_t* options,char* file_name){
  rocksdb_t *db;
  const rocksdb_writeoptions_t *woptions;
  char *err = NULL;
  kvs_tweet_t emp;
  struct timeval startTime, endTime, gepTime;
  double operating_time;


  int  num, len;
  char *buffer;  
  int size;
  int count=0;
  int total=0;
  int pos=0;

  memset(&emp.id,'\0',sizeof(emp.id));
  memset(&emp.content,'\0',sizeof(emp.content));
  gettimeofday( &startTime, NULL );

  /* DB OPEN */
  db = rocksdb_open(options, DATABASE, &err);
  assert(!err);

  woptions = rocksdb_writeoptions_create();

  //char buffer[4096];
  FILE *fp = fopen(file_name, "r");
  if(fp == NULL) {
      printf("file open error! \n");
      exit(0);
  }
  printf("fopen success! \n");

  fseek(fp, 0, SEEK_END);

  size = ftell(fp);

  buffer=malloc(size+1);
  memset(buffer, 0, size + 1);

  fseek(fp, 0, SEEK_SET);
  count = fread(buffer, size, 1, fp);

  //printf("%s\n size: %d, count: %d\n", buffer, size, count);

  //// put one by one ////



  for (int i =0; i < size+1; i++) {
    switch(buffer[i]) {
      int cc=0;
      int ss=1;

	case '<': {
    if(buffer[i+1]=='<'){char *kbegin = &buffer[i];
    char *kend = strchr(kbegin, '>');
    int kstringLength = kend - kbegin;
    memcpy(&emp.id, &buffer[i+2], kstringLength-2);
}
    else{      break;
}
break;


        //printf("key: %s\n", emp.id);
      }
      case '(': {
        if(buffer[i+1]=='('){        char *vbegin = &buffer[i];
                char *vend = strchr(vbegin, ')');
                int vstringLength = vend - vbegin;
                memcpy(&emp.content, &buffer[i+2], vstringLength-2);}
        else{
          break;
        }

        //printf("value: %s\n", emp.content);

        rocksdb_put(db, woptions,&emp.id, strlen(emp.id)+1, &emp.content, strlen(emp.content)+1, &err);
        //printf(" Record: '%s' -> '%s'\n", emp.id, emp.content);
	memset(&emp.id,'\0',sizeof(emp.id));
	//printf("%s",emp.id);
	memset(&emp.content,'\0',sizeof(emp.content));

      break;
      }
    }
  }

  /*占쎌쥙�⒳펺�뗭삕占쎌럩�뺧옙醫롫윥占쏙옙占쎌옓而양땟怨살삕筌먦룂�뺝뜝�숈삕占썩뮪�앾옙�딅뼟�좎럩鍮�옙�뗭삕占쎈맦�▼궘占쎈걠�룸냵�쇿뜝�뚯굲�좎럩堉싷옙類앸쐻占쎈뜄�ο옙���뀐옙�낅꽞嚥▲꺃彛�옙醫롫윪占쎄랬�숋옙��굲�좎럥援쏉옙�쒕쐻占쎈슢�뉛옙醫롫쑞筌뚭퍌�앭뜝占� 占쎌쥙�⒳펺�뗭삕占쎈끉��뜝�덈열占쎈틶�앾옙�덇콬�좎룞�숋옙�얠삕占쎈뜆肉댐옙醫롫윥占쎈씛�숋옙�좎굲�좎럥�놅옙�삳븸占쎌눖利닷뜝�щ걦占쎌눨�앾옙�덉굲�좎뜫�뉛옙�㏓쐻占쎌늿�뺝뜝�뚮땬壤쏆떣�숋쭗袁ⓦ럪�좎럩伊숋옙恝�숃쥈占쏀�ゅ뜝�뚯뒠占쎌궍�앾옙��뱦�좎룞��*/
  gettimeofday( &endTime, NULL );

  operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
  -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

  printf("%f\n",operating_time);
rocksdb_close(db);
free(buffer);
return operating_time;




}

double Search_Data(const rocksdb_options_t* options,char* key){
  rocksdb_t *db;
    const rocksdb_readoptions_t *roptions;
    char *err = NULL;
    kvs_tweet_t emp;
    struct timeval startTime, endTime, gepTime;
    size_t read_len;
    char *read;
    char value[2048];
    read[4096];
    read_len=sizeof(read);
    double operating_time;
     //printf("\n");



   // memset(&emp.id,'\0',strlen(emp.id));
//memset(&emp.content,0,sizeof(emp.content));

   // printf(read);

    //
    // printf("Enter Key to get :");
    // printf("\n");
    // scanf("%s",&emp.id);
    strcpy(emp.id,key);
    gettimeofday( &startTime, NULL );

    /* DB OPEN */
    db = rocksdb_open(options, DATABASE, &err);
    assert(!err);


    /*占쎌쥙�⒳펺�뗭삕占쎈끉��뜝�덈열占쎈틶�앾옙�吏뗥뜝�숈삕占쎌쥙��땟怨レ쒜占쎌럩�뺟춯癒�퉾占쎌궍�앾옙�됰뮛嶺뚯빖諭띰옙�얠삕占쎈봾堉잞옙醫롫윪��옙�숋옙��굲�좎럥留�옙�쇰쐻占쎈슣履좑옙猷뱀돸占쎌닂�숋옙占쎄뎡占쎌쥙�⑼옙�볦삕筌먦룂�뺝뜝�ш틕占쎈뀘�앾옙�덉굲 占쎌쥙�⒳펺�뗭삕占쎈끉��뜝�덈열占쎈틶�앾옙�덇콬�좎룞�숋옙�얠삕占쎈뜆肉댐옙醫롫윥占쎈씛�숋옙�좎굲�좎럥�녷뤃�먯삕�ル∥�ο옙�듭삕占쎌닂彛싷옙紐꾨㎍占쎌쥙��땟戮ル뙕占쎈쵎�삣뜝�덈쐞�담깷�앾옙��뱣�좎뜫源븝옙癒뀁삕占쏙쭗���繹먮굝�ｏ옙�놁뒻占쎌늺��쥈�밴뎡占쎌쥙�ν씙�좎럥踰⑨옙�뗭삕占쎌늿�뺝뜝�덈뼬占싸우삕占쎌뼚�뺝뜝�숈삕占쎄퍜�숅넫濡レ쭢�좎룞�� 占쎌쥙�⒳펺�뗭삕占쎈끉��뜝�덈열占쎈틶�앾옙�덇콬�좎룞�숋옙�얠삕占쎈뜆肉댐옙醫롫윥占쎈씛�숋옙�좎굲�좎럥�녷뤃�먯삕�ル∥�ε뜝�덉뵛占쎌닂�숋옙醫롪뎡占쎌쥙�ο옙�낆삕占쎈Ŋ�뺧옙�モ닪占싸룸탶野껊챶爾잌뜝�뚮땬占쎌닂�숋옙猿롫㎜占쎌쥙�⒴젆遺룸쐻占쎈챶夷⑨옙醫묒삕*/
    roptions = rocksdb_readoptions_create();

    read=rocksdb_get(db, roptions,&emp.id,strlen(emp.id)+1,&read_len, &err);
    read[read_len] = '\0';
    free(read);

    gettimeofday( &endTime, NULL );
    assert(!err);

   //   printf("Key:%s-Value: %s\n",emp.id, read);
      //printf("\n");

      //printf("\n");
    //memset(&emp.id,0,strlen(emp.id));
    //memset(&emp.content,0,sizeof(emp.content));






    operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
    -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

    //printf("%f\n",operating_time);
    rocksdb_close(db);
    return operating_time;


}
double Search_BatchData(const rocksdb_options_t* options){
  rocksdb_t *db;
  rocksdb_iterator_t *iter;
  rocksdb_readoptions_t *roptions;
  char *err = NULL;
  kvs_tweet_t emp;
  struct timeval startTime, endTime, gepTime;
  double operating_time;


  gettimeofday( &startTime, NULL );

  /* DB OPEN */
  db = rocksdb_open(options, DATABASE, &err);
  assert(!err);
  //int count=0;


  roptions = rocksdb_readoptions_create();
  iter = rocksdb_create_iterator(db, roptions);
  rocksdb_iter_seek_to_first(iter);
  int i;
  for(i=0;rocksdb_iter_valid(iter)!=0;rocksdb_iter_next(iter)){
	  //count=count+1;
	  char * str;
	  size_t len;
    assert(!err);
	  str=rocksdb_iter_key(iter,&len);
	 //printf("%s\n",str);
	  memset(str,0,len);
	  str=rocksdb_iter_value(iter,&len);
    //printf("%s\n",str);


	  //printf(str);
	  memset(str,0,len);

	  i++;
	 }


      rocksdb_iter_destroy(iter);

      gettimeofday( &endTime, NULL );

      operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
      -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

      printf("%f\n",operating_time);
    rocksdb_close(db);
    return operating_time;



}

double Delete_Data(const rocksdb_options_t* options,char* key){
  rocksdb_t *db;
  rocksdb_writeoptions_t *woptions;
  char *err = NULL;
  kvs_tweet_t emp;
  struct timeval startTime, endTime, gepTime;
  double operating_time;

  //
  // printf("KEY DELETE:\n");
  // scanf("%s",&emp.id);
  strcpy(emp.id,key);
  gettimeofday( &startTime, NULL );

  /* DB OPEN */
  db = rocksdb_open(options, DATABASE, &err);
  assert(!err);


  woptions = rocksdb_writeoptions_create();
  rocksdb_delete(db,woptions,&emp.id,strlen(emp.id)+1,&err);
  memset(&emp.id,0,sizeof(emp.id));
  memset(&emp.content,0,sizeof(emp.content));


  gettimeofday( &endTime, NULL );

  assert(!err);

  //printf("delete success\n");


      operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
      -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

      //printf("%f\n",operating_time);
rocksdb_close(db);
return operating_time;


  }

double Delete_BatchData(const rocksdb_options_t* options){
  rocksdb_t *db;
  char *err = NULL;
  kvs_tweet_t emp;
  struct timeval startTime, endTime, gepTime;
  double operating_time;


  gettimeofday( &startTime, NULL );




  rocksdb_destroy_db(options,DATABASE,&err);

  gettimeofday( &endTime, NULL );

  assert(!err);


  printf("destroy success\n");

  operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
  -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

  printf("%f\n",operating_time);
  return operating_time;


}




//#3


//#4




//#5



int main() {

    rocksdb_options_t *options;
    char *err = NULL;
    //char *read;
    int user_choice;
    int count;
    double operating_time;
    char file_batch[100];
    char file_single[100];



    struct timeval startTime, endTime, gepTime;

    /******************************************/

    /*Create*/
    gettimeofday( &startTime, NULL );
    options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(options, 1);
    gettimeofday( &endTime, NULL );

    operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
    -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

    printf("%f\n",operating_time);

    //


    while(1){
      printf("1. Delte\n");
      printf("2. Single put\n");
      printf("3. Single get\n");
      printf("4. Single delete\n");
      printf("6. Batchtest\n");
      printf("0. EXit\n");

      printf("Enter number : ");
      scanf("%d", &user_choice);

      if(user_choice == 0) {
        return 0;
      }

      printf("Enter 1: %d", count);

      experiment(options,user_choice, count);

    }



}
void experiment(const rocksdb_options_t* options,int i, int count){
  switch (i) {
    case 3:{
      double single_get=0;
      double single_delete=0;


      long a=1000000000000000000;


      char kk[2048];
      double first2=0;
      int bag;
      char kkk[2048];
      double first3=0;

         printf("single get \n");
         for(int i=0;i<100; i++){
   	      //printf("%d\n",i);
   //	      printf("for for for for\n");
   	    //  a=a+1;
   	      sprintf(kk, "%ld", a+i);
   //	      printf("sprint all\n");
   	      //printf("%f\n",Search_Data(options,kk));
   	      single_get=single_get+Search_Data(options,kk);
   	      //printf("single get %d\n",i);
   	      if(i==0) {
   	         //printf("%s \n","jiji");
   		 printf("You get 1: %f \n", single_get);
   		// printf("helllllllllo");
   		 first2=single_get;
   		// printf("heeeeeeeeeeel");
   	      } else if(i==99) {
   		 printf("Average of 100  search_siingle data is : %f\n",  single_get/100);
   		 printf("Agerage except first_experiment is : %f\n", (single_get-first2)/(99));
   	      }
   	     // printf("End %d\n",i);
         }

         printf("single delete\n ");
          for(int i=0; i<100; i++){
   	      //a=a+1;
   	      sprintf(kkk, "%ld",a+i);
   	      single_delete=single_delete+Delete_Data(options,kkk);
   	      if(i==0) {
   		  printf("You get 1: %f\n", single_delete);
   		  first3=single_delete;
   	      } else if(i==99) {
   	          printf("Average of 100  search_delete data is : %f\n",  single_delete/100);
   		  printf("Average except first_experiment is : %f\n", (single_delete-first3)/(99));
   	      }

         }

 //        batch_delete=Delete_BatchData(options);
   //      printf("Average of 100 delete_batchdata is : %f\n",  batch_delete);
         break;


    }

    case 2:{
      double single_put=0;
      char s_file[1024];
      char k[2048];
      double first1=0;

      double first =0;
      char v[2048];
      char *pStr;
      char *line_p;

      long a=1000000000000000000;

      printf("single put \n");
      printf("Enter single filename: ");
      scanf(" %[^\n]", &s_file);
      FILE *sfp = fopen(s_file, "r");
      if(sfp == NULL) {
	 printf("Fail! \n");
	 exit(0);
      }
      printf("sucess! \n");
      //printf("before for \n");
      for(int i=0;i<100; i++){
	   // printf("Start \n");

	    sprintf(k,"%ld", a+i );
	    pStr=fgets(v, sizeof(v), sfp);
	    pStr[strlen(pStr)-1]='\0';

	    single_put=single_put+Insert_Data(options,k,pStr);
           // pStr[strlen(pStr)-1]='\0';
             if(i==0){
	       printf("You get 1: %f\n", single_put);
	       first1=single_put;

	    } else if(i==99) {
	       printf("Average of 100  insert_singledata is : %f\n",  single_put/100);
	       printf("Average except first_experimnet is : %f\n", (single_put-first1)/99);

	    }
	   // printf("Success %d\n",i);

      }

      fclose(sfp);

         break;


    }
    case 1:{

	double batch_delete=0;
      char b_file[1024];
//      printf("Enter filename:\n");
  //    scanf(" %[^\n]",&b_file);
      batch_delete=Delete_BatchData(options);
      printf("Average of %d delete_batchdata is : %f\n",count,batch_delete);
	break;
    }





    case 6: {

      double batch_put=0;
      double batch_get=0;
      double batch_delete=0;
      char b_file[1024];


     // printf(strlen("1000000000000000000"));
      printf("enter batch filename:");
      scanf(" %[^\n]", &b_file);

      batch_put=Insert_BatchData(options,b_file);
      batch_get=Search_BatchData(options);

    break;
    }



  }
}
