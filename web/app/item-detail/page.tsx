// import { useEffect, useState } from "react";
import {
  Typography,
  Card,
  Stack,
  Box,
  CardContent,
  CardHeader,
  Table,
  TableBody,
  TableRow,
  TableCell,
} from "@mui/material";

type ItemData = Record<string, unknown>;

function ItemDetailAttributeCard({
  keys,
  data,
  title,
}: {
  keys: Array<string>;
  data: ItemData;
  title: string;
}) {
  return (
    <Card variant="outlined" key={"identification"} sx={{ p: 2 }}>
      <CardHeader title={title}></CardHeader>
      <CardContent>
        <Table width="max-content">
          <TableBody>
            {keys.map((element) => (
              <TableRow key={element}>
                <TableCell width={0.4}>
                  <Typography>{element}</Typography>
                </TableCell>
                <TableCell align="left">
                  <Typography>{String(data[element]) ?? ""}</Typography>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

export default async function ItemDetailPage() {
  // const [data, setData] = useState<ItemData | null>(null);
  // const [loading, setLoading] = useState(true);
  // const [error, setError] = useState<string | null>(null);

  const itemid = "9fb0e402-0ea9-4e4b-8e49-e232d77eb9a7";

  const apiUrl = process.env.API_URL
  const res = await fetch(`${apiUrl}/api/mart/item-detail?item_id=${itemid}`)
  const data: ItemData = await res.json()

  const ident_keys: Array<string> = ["id", "handle", "sku", "name"];
  const category_keys: Array<string> = ["product_category"];
  const metadata_keys: Array<string> = ["description"];
  const inventory_keys: Array<string> = [
    "inv_total",
    "track_inventory",
    "inventory_winona_avalon",
    "inventory_winona_manly",
    "inventory_winona_rozelle",
    "inventory_winona_warehouse",
    "reorder_point_winona_avalon",
    "reorder_point_winona_manly",
    "reorder_point_winona_rozelle",
    "reorder_point_winona_warehouse",
    "restock_level_winona_avalon",
    "restock_level_winona_manly",
    "restock_level_winona_rozelle",
    "restock_level_winona_warehouse",
  ];

  const pricing_keys: Array<string> = ["retail_price", "supply_price"];

  const tag_keys: Array<string> = ["tags"];
  const variant_keys: Array<string> = [
    "variant_option_one_name",
    "variant_option_one_value",
    "variant_option_three_name",
    "variant_option_three_value",
    "variant_option_two_name",
    "variant_option_two_value",
  ];

  debugger;
  return (
    <Box>
      <Typography variant="h1" sx={{ mb: 4, mt: 4}}>{String(data["name"]) ?? ""}</Typography>
      <Stack spacing={3}>
        <ItemDetailAttributeCard
          keys={ident_keys}
          data={data}
          title={"Identity"}
        />
        <ItemDetailAttributeCard
          keys={metadata_keys}
          data={data}
          title={"Metadata"}
        />
        <ItemDetailAttributeCard
          keys={category_keys}
          data={data}
          title={"Category"}
        />
        <ItemDetailAttributeCard
          keys={inventory_keys}
          data={data}
          title={"Inventory"}
        />
        <ItemDetailAttributeCard
          keys={pricing_keys}
          data={data}
          title={"Pricing"}
        />
        <ItemDetailAttributeCard keys={tag_keys} data={data} title="Tags" />
        <ItemDetailAttributeCard
          keys={variant_keys}
          data={data}
          title="Variants"
        />
      </Stack>
    </Box>
  );
}
