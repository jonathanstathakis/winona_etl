"use client";
import React from 'react';
import { Page, Text, View, Document, StyleSheet } from '@react-pdf/renderer';
import ReactDOM from 'react-dom';
import dynamic from 'next/dynamic';
  

// Use dynamic import to disable SSR for the PDF component
const PDFViewer = dynamic(
  () => import('@react-pdf/renderer').then((mod) => mod.PDFViewer),
  { ssr: false }
);
// Create styles
const styles = StyleSheet.create({
  page: {
    flexDirection: 'row',
    backgroundColor: '#E4E4E4'
  },
  section: {
    margin: 10,
    padding: 10,
    flexGrow: 1
  }
});

export default function MyDocument(){ return(
  <div style={{padding: "20px"}}>
  <PDFViewer style={{width: "50vw", height: "90vh"}}>
  <Document>
    <Page size="A4" style={styles.page}>
      <View style={styles.section}>
        <Text>Section #1</Text>
        <Text>hi this is your document.</Text>
        <Text>hi this is your document.</Text>
        <Text>hi this is your document.</Text>
        <Text>hi this is your document.</Text>
        <Text>hi this is your document.</Text>
        <Text>hi this is your document.</Text>
      </View>
    </Page>
  </Document>
  </PDFViewer>
  </div>
  )
};

