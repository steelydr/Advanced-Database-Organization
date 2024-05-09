#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_1_pagefile.txt"

/* prototypes for test functions */
static void testWriteReadBlock(void);
static void testWriteLargeBlock(void);

/* main function running all tests */
int main(void)
{
    testName = "";

    initStorageManager();
    testWriteReadBlock();
    testWriteLargeBlock();

    return 0;
}

/* Write "Hello, World!" to a block, read it, and compare */
void testWriteReadBlock(void)
{
    SM_FileHandle fh;
    SM_PageHandle ph = (SM_PageHandle)malloc(PAGE_SIZE);

    testName = "test write and read block methods";
    
    TEST_CHECK(createPageFile(TESTPF));

    TEST_CHECK(openPageFile(TESTPF, &fh));

    // Write "Hello, World!" to block 0
    strcpy(ph, "Hello, World!");
    TEST_CHECK(writeBlock(0, &fh, ph));

    // Read the content of block 0 and compare
    TEST_CHECK(readBlock(0, &fh, ph));
    ASSERT_TRUE(strcmp(ph, "Hello, World!") == 0, "content read from block 0 matches");

    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(destroyPageFile(TESTPF));
    free(ph);

    TEST_DONE();
}
/* Write a large string to a block, read it, and compare */
void testWriteLargeBlock(void)
{
    SM_FileHandle fh;
    SM_PageHandle ph = (SM_PageHandle)malloc(PAGE_SIZE);

    char str[] = "Since deep learning and machine learning tend to be used interchangeably, it’s worth noting the nuances between the two. Machine learning, deep learning, and neural networks are all sub-fields of artificial intelligence. However, neural networks is actually a sub-field of machine learning, and deep learning is a sub-field of neural networks.\nThe way in which deep learning and machine learning differ is in how each algorithm learns. Deep machine learning can use labeled datasets, also known as supervised learning, to inform its algorithm, but it doesn’t necessarily require a labeled dataset. Deep learning can ingest unstructured data in its raw form (e.g., text or images), and it can automatically determine the set of features which distinguish different categories of data from one another. This eliminates some of the human intervention required and enables the use of larger data sets. You can think of deep learning as scalable machine learning as Lex Fridman notes in this MIT lecture (link resides outside ibm.com).\nClassical, or non-deep, machine learning is more dependent on human intervention to learn. Human experts determine the set of features to understand the differences between data inputs, usually requiring more structured data to learn.Neural networks, or artificial neural networks (ANNs), are comprised of node layers, containing an input layer, one or more hidden layers, and an output layer. Each node, or artificial neuron, connects to another and has an associated weight and threshold. If the output of any individual node is above the specified threshold value, that node is activated, sending data to the next layer of the network. Otherwise, no data is passed along to the next layer of the network by that node. The “deep” in deep learning is just referring to the number of layers in a neural network. A neural network that consists of more than three layers—which would be inclusive of the input and the output—can be considered a deep learning algorithm or a deep neural network. \nA neural network that only has three layers is just a basic neural network.Deep learning and neural networks are credited with accelerating progress in areas such as computer vision, natural language processing, and speech recognition. Machine learning methods \n Machine learning models fall into three primary categories.\nSupervised machine learning \nSupervised learning, also known as supervised machine learning, is defined by its use of labeled datasets to train algorithms to classify data or predict outcomes accurately. As input data is fed into the model, the model adjusts its weights until it has been fitted appropriately. This occurs as part of the cross validation process to ensure that the model avoids overfitting or underfitting. Supervised learning helps organizations solve a variety of real-world problems at scale, such as classifying spam in a separate folder from your inbox. Some methods used in supervised learning include neural networks, naïve bayes, linear regression, logistic regression, random forest, and support vector machine (SVM).\nUnsupervised machine learning\nUnsupervised learning, also known as unsupervised machine learning, uses machine learning algorithms to analyze and cluster unlabeled datasets. These algorithms discover hidden patterns or data groupings without the need for human intervention. This method’s ability to discover similarities and differences in information make it ideal for exploratory data analysis, cross-selling strategies, customer segmentation, and image and pattern recognition. It’s also used to reduce the number of features in a model through the process of dimensionality reduction. Principal component analysis (PCA) and singular value decomposition (SVD) are two common approaches for this. Other algorithms used in unsupervised learning include neural networks, k-means clustering, and probabilistic clustering methods.\nSemi-supervised learning \nSemi-supervised learning offers a happy medium between supervised and unsupervised learning. During training, it uses a smaller labeled data set to guide classification and feature extraction from a larger, unlabeled data set. Semi-supervised learning can solve the problem of not having enough labeled data for a supervised learning algorithm. It also helps if it’s too costly to label enough data.\nCommon machine learning algorithms\nA number of machine learning algorithms are commonly used. These include:\nNeural networks: Neural networks simulate the way the human brain works, with a huge number of linked processing nodes. Neural networks are good at recognizing patterns and play an important role in applications including natural language translation, image recognition, speech recognition, and image creation.\nLinear regression: This algorithm is used to predict numerical values, based on a linear relationship between different values. For example, the technique could be used to predict house prices based on historical data for the area.\nLogistic regression: This supervised learning algorithm makes predictions for categorical response variables, such as“yes/no” answers to questions. It can be used for applications such as classifying spam and quality control on a production line.\nClustering: Using unsupervised learning, clustering algorithms can identify patterns in data so that it can be grouped. Computers can help data scientists by identifying differences between data items that humans have overlooked.\nDecision trees: Decision trees can be used for both predicting numerical values (regression) and classifying data into categories. Decision trees use a branching sequence of linked decisions that can be represented with a tree diagram. One of the advantages of decision trees is that they are easy to validate and audit, unlike the black box of the neural network.\nRandom forests: In a random forest, the machine learning algorithm predicts a value or category by combining the results from a number of decision trees.";

    
    size_t strLength = strlen(str);
    
    

    testName = "test write and read block methods";

    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));

    TEST_CHECK(writeBlock(0, &fh, str));

    TEST_CHECK(readBlock(0, &fh, ph));
    TEST_CHECK(readBlock(1, &fh, ph));

    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(destroyPageFile(TESTPF));
    free(ph);

    TEST_DONE();
}
