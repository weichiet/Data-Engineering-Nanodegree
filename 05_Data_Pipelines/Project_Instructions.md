<h1 id="project-Instructions">Project Instructions</h1>
<h2 id="project-template">Project Template</h2>
<p>To get started with the project:</p>
<ol>
<li><p>Go to the workspace on the next page, where you'll find the project template. You can work on your project and submit your work through this workspace.</p>
<p>Alternatively, you can download the <a target="_blank" href="https://s3.amazonaws.com/video.udacity-data.com/topher/2019/February/5c6058dc_project-template/project-template.zip">project template package</a> and put the contents of the package in their respective folders in your local Airflow installation.</p>
<p>The project template package contains three major components for the project:</p>
<ul>
<li>The <strong> dag template</strong> has all the imports and task templates in place, but the task dependencies have not been set</li>
<li>The <strong> operators</strong> folder with operator templates</li>
<li>A <strong> helper class</strong> for the SQL transformations</li>
</ul>
</li>
<li><p>With these template files, you should be able see the new DAG in the Airflow UI. The graph view should look like this:</p>
</li>
</ol>
</div></div><span></span></div></div></div><div><div class="index--container--2OwOl"><div class="index--atom--lmAIo layout--content--3Smmq"><div><div role="button" tabindex="0" aria-label="Show Image Fullscreen" class="image-atom--image-atom--1XDdu"><div class="image-atom-content--CDPca"><div class="image-and-annotations-container--1U01s"><img class="image--26lOQ" src="https://video.udacity-data.com/topher/2019/January/5c48b3cf_screenshot-2019-01-21-at-20.55.39/screenshot-2019-01-21-at-20.55.39.png" alt="" width="1702px"></div></div></div></div><span></span></div></div></div><div><div class="index--container--2OwOl"><div class="index--atom--lmAIo layout--content--3Smmq"><div class="ltr"><div class="index-module--markdown--2MdcR ureact-markdown "><p>You should be able to execute the DAG successfully, but if you check the logs, you will see only <code>operator not implemented</code> messages.</p>
</div></div><span></span></div></div></div><div><div class="index--container--2OwOl"><div class="index--atom--lmAIo layout--content--3Smmq"><div class="ltr"><div class="index-module--markdown--2MdcR ureact-markdown "><h2 id="configuring-the-dag">Configuring the DAG</h2>
<p>In the DAG, add <code>default parameters</code> according to these guidelines </p>
<ul>
<li>The DAG does not have dependencies on past runs</li>
<li>On failure, the task are retried 3 times</li>
<li>Retries happen every 5 minutes</li>
<li>Catchup is turned off</li>
<li>Do not email on retry</li>
</ul>
<p>In addition, configure the task dependencies so that after the dependencies are set, the graph view follows the flow shown in the image below.</p>
</div></div><span></span></div></div></div><div><div class="index--container--2OwOl"><div class="index--atom--lmAIo layout--content--3Smmq"><div><div role="button" tabindex="0" aria-label="Show Image Fullscreen" class="image-atom--image-atom--1XDdu"><div class="image-atom-content--CDPca"><div class="image-and-annotations-container--1U01s"><img class="image--26lOQ" src="https://video.udacity-data.com/topher/2019/January/5c48ba31_example-dag/example-dag.png" alt="Final DAG" width="1954px"></div><div class="caption--2IK-Y"><div class="index-module--markdown--2MdcR ureact-markdown "><p>Working DAG with correct task dependencies</p>
</div></div></div></div></div><span></span></div></div></div><div><div class="index--container--2OwOl"><div class="index--atom--lmAIo layout--content--3Smmq"><div class="ltr"><div class="index-module--markdown--2MdcR ureact-markdown "><h2 id="building-the-operators">Building the operators</h2>
<p>To complete the project, you need to build four different operators that will stage the data, transform the data, and run checks on data quality.</p>
<p>You can reuse the code from Project 2, but remember to utilize Airflow's built-in functionalities as connections and hooks as much as possible and let Airflow do all the heavy-lifting when it is possible.</p>
<p>All of the operators and task instances will run SQL statements against the Redshift database. However, using parameters wisely will allow you to build flexible, reusable, and configurable operators you can later apply to many kinds of data pipelines with Redshift and with other databases.</p>
<h3 id="stage-operator">Stage Operator</h3>
<p>The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table. </p>
<p>The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.</p>
<h3 id="fact-and-dimension-operators">Fact and Dimension Operators</h3>
<p>With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.</p>
<p>Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.</p>
<h3 id="data-quality-operator">Data Quality Operator</h3>
<p>The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually. </p>
<p>For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.</p>
<h3 id="note-about-workspace">Note about Workspace</h3>
<p>After you have updated the DAG, you will need to run <code>/opt/airflow/start.sh</code> command to start the Airflow web server. Once the Airflow web server is ready, you can access the Airflow UI by clicking on the blue <code>Access Airflow</code> button.</p>
