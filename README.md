# real-time-analysis-with-storm
use storm to analize the twitter stream and visualize the results with map and word cloud representation.
* **architecture**: 
  * *storm*
    * topology is comprised of RollingCount window, Top N tags ranking, multiple stream and stream join. 
  * *twitter4j*
    * get tweets in real time.
  * *redis*
    * publish and subscribe.
  * *flask*
    * light web framework.
  * *d3*
    * use word cloud and map for data visualization.
* **directory introduction**
  * *src*
    * java source file.
  * *viz*
    * web aplication.
